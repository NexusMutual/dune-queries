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

landed (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount) as (
  -- base: initial deposits land in their tranche
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

  union all
  -- recurse: move anything sitting in init_tranche into the extension's new_tranche (no topup here)
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

-- aggregate each extension event once, add that event's topup exactly once
ext_landings (block_time, evt_index, pool_id, token_id, tranche_id, moved_amount, stake_expiry_date) as (
  select
    l.block_time,
    l.evt_index,
    l.pool_id,
    l.token_id,
    l.tranche_id,
    sum(l.active_amount) moved_amount,
    max(l.stake_expiry_date) stake_expiry_date
  from landed l
  where l.evt_index <> -1
  group by 1, 2, 3, 4, 5
),

ext_with_topup (block_time, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select
    e.block_time,
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    coalesce(x.moved_amount, 0) + e.topup_pos active_amount,
    e.stake_expiry_date
  from ext e
  left join ext_landings x
    on x.pool_id = e.pool_id
   and x.token_id = e.token_id
   and x.block_time = e.block_time
   and x.evt_index = e.evt_index
   and x.tranche_id = e.new_tranche_id
),

-- deposits passthrough
deposit_landings (block_time, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select
    block_time,
    pool_id,
    token_id,
    cast(tranche_id as bigint) tranche_id,
    active_amount,
    stake_expiry_date
  from landed
  where evt_index = -1
),

out (block_time, pool_id, token_id, tranche_id, active_amount, stake_expiry_date) as (
  select block_time, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from deposit_landings union all
  select block_time, pool_id, token_id, tranche_id, active_amount, stake_expiry_date from ext_with_topup
)

select
  block_time,
  pool_id,
  token_id,
  tranche_id,
  active_amount,
  stake_expiry_date
from out
order by block_time, pool_id, token_id, tranche_id
