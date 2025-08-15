-- sparse time-series of active stake (deposits + extensions)
with recursive

-- extension events
ext (pool_id, token_id, block_time, evt_index, stake_expiry_date, init_tranche_id, new_tranche_id, topup_pos, tx_hash) as (
  select
    pool_id,
    token_id,
    block_time,
    evt_index,
    tranche_expiry_date as stake_expiry_date,
    init_tranche_id,
    new_tranche_id,
    coalesce(nullif(topup_amount, 0), 0) as topup_pos,
    tx_hash
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and init_tranche_id is not null
    and new_tranche_id is not null
),

-- seed 1: initial deposits (evt_index = -1 for ordering; keep real evt_index as origin key)
base_deposits (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index) as (
  select
    pool_id,
    token_id,
    tranche_id,
    block_time,
    cast(-1 as bigint) as evt_index,
    tranche_expiry_date as stake_expiry_date,
    amount as active_amount,
    tx_hash as origin_tx_hash,
    evt_index as origin_evt_index
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit'
    and tranche_id is not null
),

-- seed 2: topups as landings at their extension time (so they propagate)
base_topups (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index) as (
  select
    pool_id,
    token_id,
    new_tranche_id as tranche_id,
    block_time,
    evt_index,
    stake_expiry_date,
    topup_pos as active_amount,
    tx_hash as origin_tx_hash,
    evt_index as origin_evt_index
  from ext
  where topup_pos > 0
),

-- recursion: carry balances forward when an extension consumes init_tranche_id
landed (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index) as (
  -- seeds
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index from base_deposits union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index from base_topups union all

  -- recursive hop: move the prior landing into new_tranche (no topup added here)
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id as tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    l.active_amount,
    l.origin_tx_hash,
    l.origin_evt_index
  from landed l
  join ext e
    on e.pool_id = l.pool_id
   and e.token_id = l.token_id
   and e.init_tranche_id = l.tranche_id
   and (l.block_time < e.block_time or (l.block_time = e.block_time and l.evt_index < e.evt_index)) -- strict order
   and l.stake_expiry_date > e.block_time -- only balances still active at that moment
),

-- latest-per-origin state strictly before each extension; only count it if still in init_tranche
ext_latest (block_time, evt_index, pool_id, token_id, init_tranche_id, new_tranche_id, stake_expiry_date, topup_pos, origin_tx_hash, origin_evt_index, tranche_asof, active_asof, rn) as (
  select
    e.block_time,
    e.evt_index,
    e.pool_id,
    e.token_id,
    e.init_tranche_id,
    e.new_tranche_id,
    e.stake_expiry_date,
    e.topup_pos,
    l.origin_tx_hash,
    l.origin_evt_index,
    l.tranche_id as tranche_asof,
    l.active_amount as active_asof,
    row_number() over (
      partition by e.pool_id, e.token_id, e.block_time, e.evt_index, l.origin_tx_hash, l.origin_evt_index
      order by l.block_time desc, l.evt_index desc
    ) as rn
  from ext e
  join landed l
    on l.pool_id = e.pool_id
   and l.token_id = e.token_id
   and (l.block_time < e.block_time or (l.block_time = e.block_time and l.evt_index < e.evt_index))
   and l.stake_expiry_date > e.block_time
),

-- one landing per extension: sum latest origins that are still in init_tranche, add this event's topup
ext_landings (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    new_tranche_id as tranche_id,
    sum(case when rn = 1 and tranche_asof = init_tranche_id then active_asof else 0 end) + max(topup_pos) as active_amount,
    max(stake_expiry_date) as stake_expiry_date
  from ext_latest
  group by 1, 2, 3, 4, 5
),

-- rows that change state (deposits + extensions)
out_idx (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from base_deposits union all
  select block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from ext_landings
),

-- next extension that consumes this tranche (to set end date)
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
