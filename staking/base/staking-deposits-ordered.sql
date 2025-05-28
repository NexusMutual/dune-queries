with

deposits as (
  select
    flow_type,
    block_time,
    block_date,
    pool_id,
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
    -- token_id & flow_type
    lag(token_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_token_id,
    lag(flow_type, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_flow_type,
    lead(flow_type, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_flow_type,
    -- init_tranche_id & new_tranche_id
    lag(tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as prev_init_tranche_id,
    lag(new_tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, new_tranche_id), block_time) as prev_new_tranche_id,
    lead(tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_init_tranche_id,
    lead(tranche_id, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, new_tranche_id), block_time) as next_new_tranche_id, -- after deposit extension -> is there deposit ext addon?
    -- block_date
    lead(block_date, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as next_init_block_date,
    lead(block_date, 1) over (partition by pool_id, token_id order by coalesce(tranche_id, new_tranche_id), block_time) as next_new_block_date,
    -- deposit_rn
    row_number() over (partition by pool_id, token_id order by coalesce(tranche_id, init_tranche_id), block_time) as deposit_rn
  --from query_3609519 -- staking events - base
  from nexusmutual_ethereum.staking_events
  where flow_type in ('deposit', 'deposit extended')
),

deposits_enriched as (
  select
    block_time,
    case
      -- if there is a deposit following another deposit on the same tranche:
      when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit' and prev_init_tranche_id = tranche_id then 'deposit addon'
      -- if there is a deposit following a deposit extended on the same tranche - scenario 1:
      -- (ex token_id = 31, tx = 0x1aabd858abbb4bffb7723c81c9e81206eba924b74ce377c310728607dca5c7aa)
      when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit extended' and tranche_id = prev_new_tranche_id then 'deposit ext addon'
      -- if there is a deposit following a deposit extended on the same tranche - scenario 2:
      -- (ex token_id = 39, tx = 0x0131fe7eddf72cfca03c7926dab002061c78f145ed240139d8c2612726735a8d)
      when token_id = prev_token_id and flow_type = 'deposit' and prev_flow_type = 'deposit extended' and tranche_id = next_init_tranche_id then 'deposit ext addon'
      else flow_type
    end as flow_type,
    block_date as stake_start_date, -- stays static
    -- adjust stake_end_date to either the next deposit or the tranche expiry date
    case
      when flow_type = 'deposit extended' and next_flow_type = 'deposit' and new_tranche_id = next_new_tranche_id then coalesce(next_init_block_date, tranche_expiry_date)
      when flow_type = 'deposit' and next_flow_type <> 'deposit extended' and next_init_tranche_id <> tranche_id then tranche_expiry_date
      when flow_type = 'deposit extended' and next_flow_type = 'deposit' then tranche_expiry_date
      when next_init_block_date > tranche_expiry_date then tranche_expiry_date
      else coalesce(next_init_block_date, tranche_expiry_date)
    end as stake_end_date,
    pool_id,
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
)

select
  block_time,
  flow_type,
  stake_start_date,
  stake_end_date,
  pool_id,
  pool_address,
  token_id,
  -- for 'deposit ext addon' re-shuffle deposit-like fields to emulate deposit extension
  if(flow_type = 'deposit ext addon', null, tranche_id) as tranche_id,
  if(flow_type = 'deposit ext addon', tranche_id, init_tranche_id) as init_tranche_id,
  if(flow_type = 'deposit ext addon', tranche_id, new_tranche_id) as new_tranche_id,
  tranche_expiry_date,
  is_active,
  if(flow_type = 'deposit ext addon', null, amount) as amount,
  if(flow_type = 'deposit ext addon', amount, topup_amount) as topup_amount,
  user,
  evt_index,
  tx_hash,
  deposit_rn
from deposits_enriched
