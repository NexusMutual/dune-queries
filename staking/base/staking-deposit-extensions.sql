with recursive deposit_chain (
  block_time, pool_address, token_id, tranche_id, new_tranche_id, amount, stake_start_date, stake_end_date, is_active, evt_index, tx_hash, deposit_rn, chain_level
) as (
  select
    block_time,
    pool_address,
    token_id,
    tranche_id,
    tranche_id as new_tranche_id,
    amount,
    stake_start_date,
    stake_end_date,
    is_active,
    evt_index,
    tx_hash,
    deposit_rn,
    1 as chain_level
  from query_4102411 -- staking deposits ordered by pool_address, token_id and block_time
  where flow_type = 'deposit'
  
  union all
  
  select 
    d.block_time,
    d.pool_address,
    d.token_id,
    dc.tranche_id,
    --d.new_tranche_id,
    coalesce(d.new_tranche_id, dc.tranche_id) as new_tranche_id,
    /*case
      when d.flow_type = 'deposit extended' then d.new_tranche_id
      when d.flow_type = 'deposit' then d.tranche_id
    end as new_tranche_id,*/
    dc.amount + coalesce(d.amount, d.topup_amount, 0) as amount,
    d.stake_start_date,
    d.stake_end_date,
    d.is_active,
    d.evt_index,
    d.tx_hash,
    d.deposit_rn,
    dc.chain_level + 1 as chain_level
  from deposit_chain dc
    inner join query_4102411 d on dc.pool_address = d.pool_address and dc.token_id = d.token_id
  where dc.deposit_rn = d.deposit_rn - 1
    and ((d.flow_type = 'deposit extended' and dc.new_tranche_id = d.init_tranche_id)
      or (d.flow_type = 'deposit addon' and dc.new_tranche_id = d.tranche_id))
)

select 
  block_time,
  pool_address,
  token_id,
  tranche_id as init_tranche_id,
  new_tranche_id as current_tranche_id,
  amount,
  stake_start_date,
  stake_end_date,
  is_active,
  chain_level,
  token_tranche_rn,
  evt_index,
  tx_hash
from (
    select
      *,
      row_number() over (partition by pool_address, token_id, tranche_id order by chain_level desc) as token_tranche_rn
    from deposit_chain
  ) t
