with

staking_pools as (
  select
    sp.poolId as pool_id,
    sp.stakingPoolAddress as pool_address,
    se.first_stake_event_date
  from nexusmutual_ethereum.StakingPoolFactory_evt_StakingPoolCreated sp
    inner join (
      select
        pool_address,
        cast(min(block_time) as date) as first_stake_event_date
      --from query_3609519 -- staking events
      from nexusmutual_ethereum.staking_events
      group by 1
    ) se on sp.stakingPoolAddress = se.pool_address
),

active_stake_updated as (
  select
    asu.evt_block_date as block_date,
    sp.pool_id,
    sp.pool_address,
    asu.activeStake / 1e18 as total_staked_nxm,
    row_number() over (partition by sp.pool_id order by asu.evt_block_date) as active_stake_event_rn
  from nexusmutual_ethereum.stakingpool_evt_activestakeupdated asu
    inner join staking_pools sp on asu.contract_address = sp.pool_address
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    d.timestamp as block_date,
    if(asu.block_date is null, true, false) as is_pre_active_stake_events
  from utils.days d
    inner join staking_pools sp on d.timestamp >=sp.first_stake_event_date
    left join active_stake_updated asu
      on sp.pool_id = asu.pool_id
      and asu.active_stake_event_rn = 1
      and d.timestamp >= asu.block_date
),

deposits as (
  -- deposits & deposit extensions
  select
    d.block_date,
    d.pool_id,
    d.pool_address,
    se.current_tranche_id as tranche_id,
    sum(se.amount) as total_amount
  from staking_pool_day_sequence d
    --left join query_3619534 se -- staking deposit extensions base query
    left join nexusmutual_ethereum.staking_deposit_extensions se
      on d.pool_address = se.pool_address
      and d.block_date >= se.stake_start_date
      and d.block_date < se.stake_end_date
  where d.is_pre_active_stake_events
  group by 1, 2, 3, 4
),

burns_init as (
  select
    block_date,
    pool_id,
    pool_address,
    tranche_id,
    burn_total * staked_nxm / total_staked_nxm as burn_amount,
    staked_nxm + burn_total * staked_nxm / total_staked_nxm as new_total,
    cast(from_unixtime(91.0 * 86400.0 * cast(tranche_id + 1 as double)) as date) as tranche_expiry_date
  from (
    select
      se.block_date,
      d.pool_id,
      se.pool_address,
      d.tranche_id,
      se.amount as burn_total,
      d.staked_nxm,
      sum(d.staked_nxm) over (partition by se.pool_address, se.tranche_id) as total_staked_nxm
    from nexusmutual_ethereum.staking_events se
      inner join (
        select
          block_date,
          pool_id,
          pool_address,
          tranche_id,
          sum(coalesce(total_amount, 0)) as staked_nxm
        from deposits
        group by 1, 2, 3, 4
      ) d on se.block_date = d.block_date and se.pool_address = d.pool_address and d.tranche_id = d.tranche_id
    where se.flow_type = 'stake burn'
  ) t
),

burns as (
  select
    d.block_date,
    d.pool_id,
    d.pool_address,
    bi.tranche_id,
    sum(bi.burn_amount) as total_amount
  from staking_pool_day_sequence d
    left join burns_init bi
      on d.pool_address = bi.pool_address
      and d.block_date >= bi.block_date
      --and d.block_date < bi.tranche_expiry_date
  where d.is_pre_active_stake_events
  group by 1, 2, 3, 4
),

staked_nxm_per_pool as (
  select
    block_date,
    pool_id,
    pool_address,
    sum(coalesce(total_amount, 0)) as total_staked_nxm
  from (
      select
        block_date,
        pool_id,
        pool_address,
        tranche_id,
        total_amount
      from deposits
      union all
      select
        block_date,
        pool_id,
        pool_address,
        tranche_id,
        total_amount
      from burns
    ) t
  group by 1, 2, 3
),

staked_nxn_per_pool_combined as (
  select
    block_date,
    pool_id,
    pool_address,
    total_staked_nxm
  from staked_nxm_per_pool
  union all
  select
    block_date,
    pool_id,
    pool_address,
    total_staked_nxm
  from active_stake_updated
),

staked_nxm_per_pool_final as (
  select
    block_date,
    pool_id,
    pool_address,
    total_staked_nxm,
    row_number() over (partition by pool_id order by block_date desc) as pool_date_rn
  from staked_nxn_per_pool_combined
)

select
  block_date,
  pool_id,
  pool_address,
  total_staked_nxm,
  pool_date_rn
from staked_nxm_per_pool_final
where pool_date_rn = 1
order by pool_id, block_date
