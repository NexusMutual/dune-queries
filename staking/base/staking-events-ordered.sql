with

deposit_events as (
  select
    flow_type,
    block_time,
    date_trunc('day', block_time) as stake_start_date,
    case
      when lead(date_trunc('day', block_time), 1) over (partition by pool_address, token_id order by block_time) > tranche_expiry_date then tranche_expiry_date
      else lead(date_trunc('day', block_time), 1, tranche_expiry_date) over (partition by pool_address, token_id order by block_time)
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
    row_number() over (partition by pool_address, token_id order by block_time) as rn
    --dense_rank() over (partition by pool_address, token_id order by coalesce(tranche_id, new_tranche_id), if(tranche_id is not null, 0, 1)) as rn
  --from query_3609519 -- staking events - base
  from nexusmutual_ethereum.staking_events
  where flow_type in ('deposit', 'deposit extended')
),

deposit_events_ext as (
  select
    flow_type,
    block_time,
    stake_start_date,
    stake_end_date,
    lag(stake_end_date, 1, stake_start_date) over (partition by pool_address, token_id order by block_time) as prev_stake_end_date,
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
    rn
  from deposit_events
),

deposit_tranche_events as (
  select
    block_time,
    pool_address,
    token_id,
    dense_rank() over (partition by pool_address, token_id order by if(stake_start_date=prev_stake_end_date, 0, coalesce(tranche_id, init_tranche_id))) as tranche_rn,
    stake_start_date,
    stake_end_date,
    coalesce(amount, 0) + coalesce(topup_amount, 0) as amount
  from deposit_events_ext
)

select
  block_time,
  pool_address,
  token_id,
  stake_start_date,
  stake_end_date,
  tranche_rn,
  sum(amount) over (partition by pool_address, token_id, tranche_rn order by block_time) as amount
from deposit_tranche_events
--order by token_id, block_time
