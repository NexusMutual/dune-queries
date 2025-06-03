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

-- deposit_updates_daily 
-- commented out until deposit updates cover all active tranches for all tokens
-- also, this query needs another version with split per token and tranche

token_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    sp.token_id,
    d.timestamp as block_date,
    true as is_pre_deposit_update_events
  from utils.days d
    inner join tokens sp on d.timestamp >=sp.first_stake_event_date
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
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.token_id,
        se.current_tranche_id as tranche_id,
        sum(se.amount) as total_amount,
        max(se.stake_end_date) as stake_expiry_date
      from token_day_sequence d
        --left join nexusmutual_ethereum.base_staking_deposit_extensions se
        left join query_3619534 se -- staking deposit extensions base query
          on d.pool_id = se.pool_id
          and d.token_id = se.token_id
          and d.block_date >= se.stake_start_date
          and d.block_date < se.stake_end_date
      --where d.is_pre_deposit_update_events -- as per ** comment below
      group by 1, 2, 3, 4, 5
      union all
      -- withdrawals & burns
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.token_id,
        se.tranche_id,
        sum(se.amount) as total_amount,
        cast(null as date) as stake_expiry_date -- no point pulling stake_expiry_date for withdrawals
      from token_day_sequence d
        inner join nexusmutual_ethereum.staking_events se
        --inner join query_3609519 se -- staking events
          on d.pool_id = se.pool_id
          and d.token_id = se.token_id
          and d.block_date >= se.block_date
          and d.block_date < coalesce(se.tranche_expiry_date, current_date)
      where se.flow_type = 'withdraw' -- token_id is null on 'stake burn'
        --and d.is_pre_deposit_update_events -- as per ** comment below
      group by 1, 2, 3, 4, 5
    ) t
  where token_id is not null
  group by 1, 2, 3, 4, 5
),

staked_nxm_per_token_tranche_combined as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    tranche_id,
    total_staked_nxm,
    stake_expiry_date
  from staked_nxm_per_token_tranche
  /*
  -- ** commented out until deposit updates cover all active tranches for all tokens
  union all
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    tranche_id,
    total_staked_nxm,
    stake_expiry_date
  from deposit_updates_daily*/
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
  from staked_nxm_per_token_tranche_combined
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
