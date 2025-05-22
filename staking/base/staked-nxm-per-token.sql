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

deposit_updates_daily as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    token_stake as total_staked_nxm,
    tranche_expiry_date as stake_expiry_date,
    row_number() over (partition by pool_id, token_id order by block_date) as deposit_update_event_rn
  from nexusmutual_ethereum.base_deposit_updates_daily
  --from query_5156273 -- deposit updates daily - base
),

token_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    coalesce(sp.token_id, du.token_id) as token_id,
    d.timestamp as block_date,
    if(du.block_date is null, true, false) as is_pre_deposit_update_events
  from utils.days d
    inner join tokens sp on d.timestamp >=sp.first_stake_event_date
    left join deposit_updates_daily du
      on sp.pool_id = du.pool_id
      and sp.token_id = du.token_id
      and du.deposit_update_event_rn = 1
      and d.timestamp >= du.block_date
  where coalesce(sp.token_id, du.token_id) is not null
),

staked_nxm_per_pool_n_token as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    sum(coalesce(total_amount, 0)) as total_staked_nxm,
    max(stake_expiry_date) as stake_expiry_date
  from (
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.token_id,
        sum(se.amount) as total_amount,
        max(se.stake_end_date) as stake_expiry_date
      from token_day_sequence d
        inner join nexusmutual_ethereum.base_staking_deposit_extensions se
        --inner join query_3619534 se -- staking deposit extensions base query
          on d.pool_id = se.pool_id
          and d.token_id = se.token_id
          and d.block_date >= se.stake_start_date
          and d.block_date < se.stake_end_date
      where d.is_pre_deposit_update_events
      group by 1, 2, 3, 4
      union all
      -- withdrawals & burns
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.token_id,
        sum(se.amount) as total_amount,
        cast(null as date) as stake_expiry_date -- no point pulling stake_expiry_date for withdrawals
      from token_day_sequence d
        inner join nexusmutual_ethereum.staking_events se
        --inner join query_3609519 se -- staking events
          on d.pool_id = se.pool_id
          and d.token_id = se.token_id
          and d.block_date >= se.block_date
          and d.block_date < coalesce(se.tranche_expiry_date, current_date)
      where se.flow_type in ('withdraw', 'stake burn')
        and d.is_pre_deposit_update_events
      group by 1, 2, 3, 4
    ) t
  group by 1, 2, 3, 4
),

staked_nxm_per_pool_n_token_combined as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    total_staked_nxm,
    stake_expiry_date
  from staked_nxm_per_pool_n_token
  union all
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    total_staked_nxm,
    stake_expiry_date
  from deposit_updates_daily
),

staked_nxm_per_pool_n_token_final as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    total_staked_nxm,
    stake_expiry_date,
    row_number() over (partition by pool_id, token_id order by block_date desc) as token_date_rn
  from staked_nxm_per_pool_n_token_combined
)

select
  block_date,
  pool_id,
  pool_address,
  token_id,
  total_staked_nxm,
  stake_expiry_date,
  token_date_rn
from staked_nxm_per_pool_n_token_final
--where token_date_rn = 1 and block_date = current_date
--order by pool_id, block_date
