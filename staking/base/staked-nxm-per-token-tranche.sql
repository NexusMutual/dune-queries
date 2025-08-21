with

tokens as (
  select
    sp.pool_id,
    sp.pool_address,
    se.token_id,
    se.first_stake_event_date
  from nexusmutual_ethereum.staking_pools_list sp
  --from query_5128062 sp -- staking pools list - base query
    inner join (
      select
        pool_id,
        token_id,
        cast(min(block_time) as date) as first_stake_event_date
      from nexusmutual_ethereum.staking_events
      --from query_3609519 -- staking events
      group by 1, 2
    ) se on sp.pool_id = se.pool_id
),

token_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    sp.token_id,
    d.timestamp as block_date
  from utils.days d
    inner join tokens sp on d.timestamp >= sp.first_stake_event_date
),

staked_nxm_per_token_tranche as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    tranche_id,
    sum(coalesce(total_amount, 0)) as total_staked_nxm,
    max(stake_expiry_date) as stake_expiry_date
  from (
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        sd.token_id,
        sd.tranche_id,
        sum(sd.active_amount) as total_amount,
        max(sd.stake_expiry_date) as stake_expiry_date
      from token_day_sequence d
        left join query_5651171 sd -- staking deposits with burns - base
          on d.pool_id = sd.pool_id
          and d.token_id = sd.token_id
          and d.block_date >= sd.stake_start_date
          and d.block_date < sd.stake_end_date
      group by 1, 2, 3, 4, 5
    ) t
  where token_id is not null
  group by 1, 2, 3, 4, 5
),

staked_nxm_per_token_tranche_final as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    tranche_id,
    total_staked_nxm,
    stake_expiry_date,
    row_number() over (partition by pool_id, token_id, tranche_id order by block_date desc) as token_tranche_rn
  from staked_nxm_per_token_tranche
)

select
  block_date,
  pool_id,
  pool_address,
  token_id,
  tranche_id,
  total_staked_nxm,
  stake_expiry_date,
  token_tranche_rn
from staked_nxm_per_token_tranche_final
--where token_tranche_rn = 1 and block_date = current_date
--order by pool_id, block_date
