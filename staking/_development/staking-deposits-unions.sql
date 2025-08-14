with

deposits as (
  select
    pool_id,
    token_id,
    cast(tranche_id as bigint) tranche_id,
    cast(block_time as timestamp) block_time,
    cast(-1 as bigint) evt_index,
    cast(tranche_expiry_date as timestamp) stake_expiry_date,
    cast(amount as double) active_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit' and tranche_id is not null
),

ext as (
  select
    pool_id,
    token_id,
    cast(block_time as timestamp) block_time,
    cast(evt_index as bigint) evt_index,
    cast(tranche_expiry_date as timestamp) stake_expiry_date,
    cast(init_tranche_id as bigint) init_tranche_id,
    cast(new_tranche_id as bigint) new_tranche_id,
    cast(coalesce(topup_amount, 0) as double) topup_pos
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and init_tranche_id is not null
    and new_tranche_id is not null
),

lvl0 as (
  select
    pool_id,
    token_id,
    tranche_id,
    block_time,
    evt_index,
    stake_expiry_date,
    active_amount
  from deposits
),

lvl1 as (
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    sum(s.active_amount) + max(e.topup_pos) active_amount
  from ext e
  join lvl0 s
    on s.pool_id = e.pool_id
   and s.token_id = e.token_id
   and s.tranche_id = e.init_tranche_id
   and (s.block_time < e.block_time or (s.block_time = e.block_time and s.evt_index < e.evt_index))
   and s.stake_expiry_date > e.block_time
  group by 1, 2, 3, 4, 5, 6
),

lvl2 as (
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    sum(s.active_amount) + max(e.topup_pos) active_amount
  from ext e
  join lvl1 s
    on s.pool_id = e.pool_id
   and s.token_id = e.token_id
   and s.tranche_id = e.init_tranche_id
   and (s.block_time < e.block_time or (s.block_time = e.block_time and s.evt_index < e.evt_index))
   and s.stake_expiry_date > e.block_time
  group by 1, 2, 3, 4, 5, 6
),

lvl3 as (
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    sum(s.active_amount) + max(e.topup_pos) active_amount
  from ext e
  join lvl2 s
    on s.pool_id = e.pool_id
   and s.token_id = e.token_id
   and s.tranche_id = e.init_tranche_id
   and (s.block_time < e.block_time or (s.block_time = e.block_time and s.evt_index < e.evt_index))
   and s.stake_expiry_date > e.block_time
  group by 1, 2, 3, 4, 5, 6
),

lvl4 as (
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    sum(s.active_amount) + max(e.topup_pos) active_amount
  from ext e
  join lvl3 s
    on s.pool_id = e.pool_id
   and s.token_id = e.token_id
   and s.tranche_id = e.init_tranche_id
   and (s.block_time < e.block_time or (s.block_time = e.block_time and s.evt_index < e.evt_index))
   and s.stake_expiry_date > e.block_time
  group by 1, 2, 3, 4, 5, 6
),

lvl5 as (
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    sum(s.active_amount) + max(e.topup_pos) active_amount
  from ext e
  join lvl4 s
    on s.pool_id = e.pool_id
   and s.token_id = e.token_id
   and s.tranche_id = e.init_tranche_id
   and (s.block_time < e.block_time or (s.block_time = e.block_time and s.evt_index < e.evt_index))
   and s.stake_expiry_date > e.block_time
  group by 1, 2, 3, 4, 5, 6
),

lvl6 as (
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    sum(s.active_amount) + max(e.topup_pos) active_amount
  from ext e
  join lvl5 s
    on s.pool_id = e.pool_id
   and s.token_id = e.token_id
   and s.tranche_id = e.init_tranche_id
   and (s.block_time < e.block_time or (s.block_time = e.block_time and s.evt_index < e.evt_index))
   and s.stake_expiry_date > e.block_time
  group by 1, 2, 3, 4, 5, 6
),

landings as (
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from lvl0 union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from lvl1 union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from lvl2 union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from lvl3 union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from lvl4 union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from lvl5 union all
  select pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount from lvl6
),

collapsed as (
  select
    block_time,
    pool_id,
    token_id,
    cast(tranche_id as bigint) tranche_id,
    sum(active_amount) active_amount,
    max(stake_expiry_date) stake_expiry_date
  from landings
  group by 1, 2, 3, 4
)

select
  block_time,
  pool_id,
  token_id,
  tranche_id,
  active_amount,
  stake_expiry_date
from collapsed
order by block_time, pool_id, token_id, tranche_id
