/*
builds a sparse time-series of active stake per (pool_id, token_id, tranche_id)
events handled:
- deposits → seed balances
- deposit extensions → move balances to new_tranche_id (and add topups)
- withdrawals → negative seeds at that tranche/time (reduce balances going forward)
ordering: strictly by (block_time, evt_index)
chaining rule: each landing hops ONLY to the earliest next extension that consumes its tranche
dates:
- stake_start_date = date(block_time)
- stake_end_date   = date(next extension that consumes this tranche) else date(stake_expiry_date)
*/

with recursive

-- deposit-extension edges (including topup at the event)
ext (pool_id, token_id, block_time, evt_index, stake_expiry_date, init_tranche_id, new_tranche_id, topup_pos) as (
  select
    pool_id,
    token_id,
    block_time,
    evt_index,
    tranche_expiry_date as stake_expiry_date,
    init_tranche_id,
    new_tranche_id,
    coalesce(nullif(topup_amount, 0), 0) as topup_pos
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and init_tranche_id is not null
    and new_tranche_id is not null
),

-- initial deposits (evt_index = -1 ensures deposits sort before same-timestamp extensions)
base_deposits (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  select
    pool_id,
    token_id,
    tranche_id,
    block_time,
    cast(-1 as bigint) as evt_index,
    tranche_expiry_date as stake_expiry_date,
    amount as active_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit'
    and tranche_id is not null
),

-- topups materialized as landings at the extension event so they propagate to later hops
base_topups (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  select
    pool_id,
    token_id,
    new_tranche_id as tranche_id,
    block_time,
    evt_index,
    stake_expiry_date,
    topup_pos as active_amount
  from ext
  where topup_pos > 0
),

-- withdrawals as negative landings on that token+tranche at the event time
-- note: assumes withdraw 'amount' is already signed (negative) in source; if positive, use -abs(amount)
base_withdraws (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  select
    pool_id,
    token_id,
    tranche_id,
    block_time,
    evt_index,
    tranche_expiry_date as stake_expiry_date,
    amount as active_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'withdraw'
    and tranche_id is not null
),

-- recursion: carry each landing forward to ONLY the earliest next extension that consumes its tranche
-- prevents stale re-carries and enforces strict event order; expired balances do not move
landed (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  -- seeds: deposits, topups, withdrawals
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from base_deposits union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from base_topups union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from base_withdraws union all

  -- hop to the first extension strictly after this landing (no topup added here)
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id as tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    l.active_amount
  from landed l
  join ext e
    on e.pool_id = l.pool_id
   and e.token_id = l.token_id
   and e.init_tranche_id = l.tranche_id
   and (l.block_time < e.block_time or (l.block_time = e.block_time and l.evt_index < e.evt_index)) -- strict order
   and l.stake_expiry_date > e.block_time -- only still-active balances can move
  where not exists ( -- ensure e is the earliest next extension after l for this (pool, token, tranche)
    select 1
    from ext e2
    where e2.pool_id = e.pool_id
      and e2.token_id = e.token_id
      and e2.init_tranche_id = e.init_tranche_id
      and (l.block_time < e2.block_time or (l.block_time = e2.block_time and l.evt_index < e2.evt_index)) -- after l
      and (e2.block_time < e.block_time or (e2.block_time = e.block_time and e2.evt_index < e.evt_index)) -- before e
  )
),

-- one landing per extension event: sum moved amounts + the topup seed at that event
-- (join on the event keys so only the extension’s landings are aggregated)
ext_landings (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select
    e.block_time,
    e.evt_index,
    e.pool_id,
    e.token_id,
    e.new_tranche_id as tranche_id,
    sum(l.active_amount) as active_amount,
    max(e.stake_expiry_date) as stake_expiry_date
  from ext e
  join landed l
    on l.pool_id = e.pool_id
   and l.token_id = e.token_id
   and l.block_time = e.block_time
   and l.evt_index = e.evt_index
   and l.tranche_id = e.new_tranche_id
  group by 1, 2, 3, 4, 5
),

-- rows that change state and should appear in the sparse series (deposits + extensions)
-- note: withdrawals are not emitted as rows; they reduce amounts that feed into future extensions
out_idx (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from base_deposits union all
  select block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from ext_landings
),

-- next extension that consumes this tranche (for stake_end_date)
next_ext_candidates (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date, next_block_time, next_evt_index, rn) as (
  select
    o.block_time,
    o.evt_index,
    o.pool_id,
    o.token_id,
    o.tranche_id,
    o.active_amount,
    o.stake_expiry_date,
    e.block_time as next_block_time,
    e.evt_index as next_evt_index,
    row_number() over (
      partition by o.block_time, o.evt_index, o.pool_id, o.token_id, o.tranche_id
      order by e.block_time, e.evt_index
    ) as rn
  from out_idx o
  left join ext e
    on e.pool_id = o.pool_id
   and e.token_id = o.token_id
   and e.init_tranche_id = o.tranche_id
   and (e.block_time > o.block_time or (e.block_time = o.block_time and e.evt_index > o.evt_index))
)

select
  block_time,
  pool_id,
  token_id,
  tranche_id,
  active_amount,
  date(block_time) as stake_start_date,
  coalesce(date(next_block_time), date(stake_expiry_date)) as stake_end_date,
  date(stake_expiry_date) as stake_expiry_date
from next_ext_candidates
where rn = 1
order by 1, 2, 3, 4
