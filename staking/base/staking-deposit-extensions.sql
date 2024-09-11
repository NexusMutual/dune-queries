-- query_3609519: staking events - base

with recursive deposit_chain (pool_address, token_id, tranche_id, new_tranche_id, total_amount, block_time, tranche_expiry_date, is_active, evt_index, tx_hash, chain_level) as (
  select
    pool_address,
    token_id,
    tranche_id as tranche_id,
    tranche_id as new_tranche_id,
    sum(amount) as total_amount,
    max(block_time) as block_time,
    max_by(tranche_expiry_date, block_time) as tranche_expiry_date,
    max_by(is_active, block_time) as is_active,
    max_by(evt_index, block_time) as evt_index,
    max_by(tx_hash, block_time) as tx_hash,
    1 as chain_level
  from query_3609519
  where flow_type = 'deposit'
  group by 1,2,3,4
  
  union all
  
  select 
    d.pool_address,
    d.token_id,
    dc.tranche_id,
    d.new_tranche_id,
    dc.total_amount + coalesce(d.topup_amount, 0) as total_amount,
    d.block_time,
    d.tranche_expiry_date,
    d.is_active,
    d.evt_index,
    d.tx_hash,
    dc.chain_level + 1 as chain_level
  from deposit_chain dc
  join query_3609519 d on dc.pool_address = d.pool_address
    and dc.token_id = d.token_id
    and dc.new_tranche_id = d.init_tranche_id
  where d.flow_type = 'deposit extended'
)

select 
  block_time,
  pool_address,
  token_id,
  tranche_id as init_tranche_id,
  new_tranche_id as current_tranche_id,
  total_amount,
  tranche_expiry_date,
  is_active,
  chain_level,
  rn as token_tranche_rn,
  evt_index,
  tx_hash
from (
    select
      *,
      row_number() over (partition by pool_address, token_id, tranche_id order by chain_level desc) as rn
    from deposit_chain
  ) t
--where rn = 1
