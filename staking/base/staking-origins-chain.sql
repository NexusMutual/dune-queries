/*
per-origin staking origins chain (deposits + extensions only, no burns)
- purpose: sparse event-level series per origin used as the base for burns/time-series
- rules:
  * deposits land in their tranche (evt_index = -1 to order before same-ts extensions)
  * extension moves prior tranche balance to new_tranche once (earliest next extension only)
  * topups at extension become new origins that propagate forward
  * stake windows are [stake_start_date, stake_end_date), ending at the next consuming extension or expiry
*/

with recursive

-- extensions (including topup amount and new expiry)
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
  from query_5734582 -- staking events - base root
  where flow_type = 'deposit extended'
),

-- initial deposits as origins (evt_index = -1 to sort before same-ts extensions)
base_deposits (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index, origin_block_time) as (
  select
    pool_id,
    token_id,
    tranche_id,
    block_time,
    cast(-1 as bigint) as evt_index,
    tranche_expiry_date as stake_expiry_date,
    amount as active_amount,
    tx_hash as origin_tx_hash,
    evt_index as origin_evt_index,
    block_time as origin_block_time
  from query_5734582 -- staking events - base root
  where flow_type = 'deposit'
),

-- topups at extension are new origins that start at the extension ts
base_topups (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index, origin_block_time) as (
  select
    pool_id,
    token_id,
    new_tranche_id as tranche_id,
    block_time,
    evt_index,
    stake_expiry_date,
    topup_pos as active_amount,
    tx_hash as origin_tx_hash,
    evt_index as origin_evt_index,
    block_time as origin_block_time
  from ext
  where topup_pos > 0
),

-- propagate each origin along the extension path (earliest next extension only)
landed (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, active_amount, origin_tx_hash, origin_evt_index, origin_block_time) as (
  -- seeds (deposits + topups)
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

  -- recursive hop to the next consuming extension for this tranche
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
   and l.stake_expiry_date > e.block_time
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

-- extension landings per origin (evt rows only; deposits handled separately)
ext_landings (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date, origin_tx_hash, origin_evt_index, origin_block_time) as (
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
  from landed
  where evt_index <> -1
),

-- rows that change state (deposits + extensions), per origin
out_idx (block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date, origin_tx_hash, origin_evt_index, origin_block_time) as (
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

-- find the next extension that consumes this tranche (to set stake_end_date)
next_ext_candidates (
  block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_expiry_date,
  origin_tx_hash, origin_evt_index, origin_block_time, next_block_time, next_evt_index, rn
) as (
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
    e.block_time as next_block_time,
    e.evt_index as next_evt_index,
    row_number() over (
      partition by o.block_time, o.evt_index, o.pool_id, o.token_id, o.tranche_id, o.origin_tx_hash, o.origin_evt_index
      order by e.block_time, e.evt_index
    ) as rn
  from out_idx o
  left join ext e
    on e.pool_id = o.pool_id
   and e.token_id = o.token_id
   and e.init_tranche_id = o.tranche_id
   and (e.block_time > o.block_time or (e.block_time = o.block_time and e.evt_index > o.evt_index))
),

-- pick the earliest next extension (or none) and compute stake window
out (
  block_time, evt_index, pool_id, token_id, tranche_id, active_amount, stake_start_date, stake_end_date, stake_expiry_date, 
  origin_tx_hash, origin_evt_index, origin_block_time
) as (
  select
    n.block_time,
    n.evt_index,
    n.pool_id,
    n.token_id,
    n.tranche_id,
    n.active_amount,
    date(n.block_time) as stake_start_date,
    coalesce(date(n.next_block_time), date(n.stake_expiry_date)) as stake_end_date,
    date(n.stake_expiry_date) as stake_expiry_date,
    n.origin_tx_hash,
    n.origin_evt_index,
    n.origin_block_time
  from next_ext_candidates n
  where n.rn = 1
)

-- final output (sparse per-origin chain)
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
  origin_block_time,
  stake_start_date,
  stake_end_date
from out
order by 1, 2, 3, 4
