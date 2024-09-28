with

deposits as (
  select
    flow_type,
    block_time,
    date_trunc('day', block_time) as block_date,
    pool_address,
    token_id,
    tranche_id,
    init_tranche_id,
    new_tranche_id,
    tranche_expiry_date,
    is_active,
    amount,
    topup_amount,
    user,
    evt_index,
    tx_hash,
    lead(date_trunc('day', block_time), 1) over (partition by pool_address, token_id order by block_time, tranche_id) as next_block_date,
    lag(token_id, 1) over (partition by pool_address, token_id order by block_time, tranche_id) as prev_token_id,
    lag(flow_type, 1) over (partition by pool_address, token_id order by block_time, tranche_id) as prev_flow_type,
    lag(tranche_expiry_date, 1) over (partition by pool_address, token_id order by block_time, tranche_id) as prev_tranche_expiry_date,
    row_number() over (partition by pool_address, token_id order by block_time, tranche_id) as deposit_rn
  --from query_3609519 -- staking events - base
  from nexusmutual_ethereum.staking_events
  where flow_type in ('deposit', 'deposit extended')
)

select
  block_time,
  case
    when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit' and prev_tranche_expiry_date > block_date
    then 'deposit addon'
    else flow_type
  end as flow_type,
  block_date as stake_start_date,
  case
    when next_block_date > tranche_expiry_date then tranche_expiry_date
    else coalesce(next_block_date, tranche_expiry_date)
  end as stake_end_date,
  pool_address,
  token_id,
  tranche_id,
  init_tranche_id,
  new_tranche_id,
  tranche_expiry_date,
  is_active,
  amount,
  topup_amount,
  user,
  evt_index,
  tx_hash,
  deposit_rn
from deposits
