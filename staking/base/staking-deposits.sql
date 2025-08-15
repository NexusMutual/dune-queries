with recursive

ext (pool_id, token_id, block_time, evt_index, stake_expiry_date, init_tranche_id, new_tranche_id, topup_pos) as (
  select
    pool_id,
    token_id,
    cast(block_time as timestamp) block_time,
    cast(evt_index as bigint) evt_index,
    cast(tranche_expiry_date as timestamp) stake_expiry_date,
    cast(init_tranche_id as bigint) init_tranche_id,
    cast(new_tranche_id as bigint) new_tranche_id,
    cast(coalesce(nullif(topup_amount, 0), 0) as double) topup_pos
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and init_tranche_id is not null
    and new_tranche_id is not null
),

base_deposits (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  select
    pool_id,
    token_id,
    cast(tranche_id as bigint) tranche_id,
    cast(block_time as timestamp) block_time,
    cast(-1 as bigint) evt_index,
    cast(tranche_expiry_date as timestamp) stake_expiry_date,
    cast(amount as double) active_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit'
    and tranche_id is not null
),

base_topups (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  select
    pool_id,
    token_id,
    new_tranche_id tranche_id,
    block_time,
    evt_index,
    stake_expiry_date,
    topup_pos active_amount
  from ext
  where topup_pos > 0
),

landed (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  -- non-recursive seeds: deposits and topups
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from base_deposits union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from base_topups union all

  -- recursive step: move anything sitting in init_tranche into the extension's new_tranche (no topup here)
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    l.active_amount
  from landed l
  join ext e
    on e.pool_id = l.pool_id
   and e.token_id = l.token_id
   and e.init_tranche_id = l.tranche_id
   and (l.block_time < e.block_time or (l.block_time = e.block_time and l.evt_index < e.evt_index))
   and l.stake_expiry_date > e.block_time
),

-- aggregate each extension event to one row (moved amounts + topup seed), keep deposits as-is
ext_landings (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    sum(active_amount) active_amount,
    max(stake_expiry_date) stake_expiry_date
  from landed
  where evt_index <> -1
  group by 1, 2, 3, 4, 5
),

deposit_landings (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    active_amount,
    stake_expiry_date
  from base_deposits
),

out_idx (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from deposit_landings union all
  select block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from ext_landings
),

-- next extension that uses this row's tranche as init_tranche_id
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
    ) rn
  from out_idx o
  left join ext e
    on e.pool_id = o.pool_id
   and e.token_id = o.token_id
   and e.init_tranche_id = o.tranche_id
   and (e.block_time > o.block_time or (e.block_time = o.block_time and e.evt_index > o.evt_index))
),

next_ext (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date, next_block_time, next_evt_index) as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    active_amount,
    stake_expiry_date,
    next_block_time,
    next_evt_index
  from next_ext_candidates
  where rn = 1
),

out (block_time, pool_id, token_id, tranche_id, active_amount, stake_start_date, stake_end_date, stake_expiry_date) as (
  select
    n.block_time,
    n.pool_id,
    n.token_id,
    n.tranche_id,
    n.active_amount,
    date(n.block_time) as stake_start_date,
    coalesce(date(n.next_block_time), date(n.stake_expiry_date)) as stake_end_date,
    n.stake_expiry_date
  from next_ext n
)

select
  block_time,
  pool_id,
  token_id,
  tranche_id,
  active_amount,
  stake_start_date,
  stake_end_date,
  stake_expiry_date
from out
order by 1, 2, 3, 4
