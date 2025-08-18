with recursive

-- extension edges
ext (pool_id, token_id, block_time, evt_index, init_tranche_id, new_tranche_id, stake_expiry_date) as (
  select
    pool_id,
    token_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    cast(init_tranche_id as bigint) as init_tranche_id,
    cast(new_tranche_id as bigint) as new_tranche_id,
    cast(tranche_expiry_date as date) as stake_expiry_date
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and init_tranche_id is not null
    and new_tranche_id is not null
),

-- deposits = origin seeds
base_deposits (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index, origin_block_time) as (
  select
    pool_id,
    token_id,
    cast(tranche_id as bigint) as tranche_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    cast(tranche_expiry_date as date) as stake_expiry_date,
    amount as active_amount,
    tx_hash as origin_tx_hash,
    cast(evt_index as bigint) as origin_evt_index,
    block_time as origin_block_time
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit'
    and tranche_id is not null
),

-- topups = separate origins at the extension event
base_topups (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index, origin_block_time) as (
  select
    pool_id,
    token_id,
    cast(new_tranche_id as bigint) as tranche_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    cast(tranche_expiry_date as date) as stake_expiry_date,
    coalesce(nullif(topup_amount, 0), 0) as active_amount,
    tx_hash as origin_tx_hash,
    cast(evt_index as bigint) as origin_evt_index,
    block_time as origin_block_time
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and new_tranche_id is not null
    and coalesce(nullif(topup_amount, 0), 0) > 0
),

-- carry each origin to the earliest next extension that consumes its tranche
landed (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index, origin_block_time) as (
  select
    pool_id,
    token_id,
    tranche_id,
    block_time,
    evt_index,
    stake_expiry_date,
    active_amount,
    origin_tx_hash,
    origin_evt_index,
    origin_block_time
  from base_deposits

  union all
  select
    pool_id,
    token_id,
    tranche_id,
    block_time,
    evt_index,
    stake_expiry_date,
    active_amount,
    origin_tx_hash,
    origin_evt_index,
    origin_block_time
  from base_topups

  union all
  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id as tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    l.active_amount,
    l.origin_tx_hash,
    l.origin_evt_index,
    l.origin_block_time
  from landed l
  join ext e
    on e.pool_id = l.pool_id
   and e.token_id = l.token_id
   and e.init_tranche_id = l.tranche_id
   and (l.block_time < e.block_time or (l.block_time = e.block_time and l.evt_index < e.evt_index))
   and l.stake_expiry_date > date(e.block_time)
  where not exists (
    select 1
    from ext e2
    where e2.pool_id = e.pool_id
      and e2.token_id = e.token_id
      and e2.init_tranche_id = e.init_tranche_id
      and (l.block_time < e2.block_time or (l.block_time = e2.block_time and l.evt_index < e2.evt_index))
      and (e2.block_time < e.block_time or (e2.block_time = e.block_time and e2.evt_index < e.evt_index))
  )
),

-- emit per-origin rows at extension events (moved origins)
ext_landings (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date, origin_tx_hash, origin_evt_index, origin_block_time) as (
  select
    e.block_time,
    e.evt_index,
    e.pool_id,
    e.token_id,
    e.new_tranche_id as tranche_id,
    l.active_amount,
    e.stake_expiry_date,
    l.origin_tx_hash,
    l.origin_evt_index,
    l.origin_block_time
  from ext e
  join landed l
    on l.pool_id = e.pool_id
   and l.token_id = e.token_id
   and l.block_time = e.block_time
   and l.evt_index = e.evt_index
   and l.tranche_id = e.new_tranche_id
),

-- rows to publish (deposit origins + extension landings), plus next extension pointer
out_rows_base (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date, origin_tx_hash, origin_evt_index, origin_block_time) as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    active_amount,
    stake_expiry_date,
    origin_tx_hash,
    origin_evt_index,
    origin_block_time
  from base_deposits

  union all
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    active_amount,
    stake_expiry_date,
    origin_tx_hash,
    origin_evt_index,
    origin_block_time
  from ext_landings
),

next_ext (block_time, evt_index, pool_id, token_id, tranche_id, origin_tx_hash, origin_evt_index, next_block_time, next_evt_index) as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    origin_tx_hash,
    origin_evt_index,
    next_block_time,
    next_evt_index
  from (
    select
      o.block_time,
      o.evt_index,
      o.pool_id,
      o.token_id,
      o.tranche_id,
      o.origin_tx_hash,
      o.origin_evt_index,
      e.block_time as next_block_time,
      e.evt_index as next_evt_index,
      row_number() over (
        partition by o.block_time, o.evt_index, o.pool_id, o.token_id, o.tranche_id, o.origin_tx_hash, o.origin_evt_index
        order by e.block_time, e.evt_index
      ) as rn
    from out_rows_base o
    left join ext e
      on e.pool_id = o.pool_id
     and e.token_id = o.token_id
     and e.init_tranche_id = o.tranche_id
     and (e.block_time > o.block_time or (e.block_time = o.block_time and e.evt_index > o.evt_index))
  ) t
  where rn = 1
)

select
  o.block_time,
  o.evt_index,
  o.pool_id,
  o.token_id,
  o.tranche_id,
  o.active_amount,
  o.stake_expiry_date,
  o.origin_tx_hash,
  o.origin_evt_index,
  o.origin_block_time,
  date(o.block_time) as stake_start_date,
  coalesce(date(n.next_block_time), o.stake_expiry_date) as stake_end_date,
  n.next_block_time,
  n.next_evt_index
from out_rows_base o
left join next_ext n
  on n.pool_id = o.pool_id
 and n.token_id = o.token_id
 and n.tranche_id = o.tranche_id
 and n.block_time = o.block_time
 and n.evt_index = o.evt_index
 and n.origin_tx_hash = o.origin_tx_hash
 and n.origin_evt_index = o.origin_evt_index
