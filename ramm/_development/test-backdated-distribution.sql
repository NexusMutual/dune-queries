with

active as (
  select
    staker_address as staker,
    pool_id,
    token_id,
    staked_nxm as nxm_active_stake
  from query_4077503
),

staking_rewards as (
  select
    month_date,
    pool_id,
    pool_name,
    nxm_reward_total
  from dune.nexus_mutual.result_staking_rewards_monthly
  where month_date >= timestamp '2024-01-01'
),

ramm_added as (
  select
    block_month,
    bv_diff_nxm
  from dune.nexus_mutual.result_ramm_monthly_value_accrual
),

pool_ma as (
  select
    month_date,
    pool_id,
    pool_name,
    avg(nxm_reward_total) over (
      partition by pool_id
      order by month_date
      rows between 2 preceding and current row
    ) as nxm_reward_3m_moving_avg
  from staking_rewards
),

month_totals as (
  select
    month_date,
    sum(greatest(nxm_reward_3m_moving_avg, 0)) as month_ma_total
  from pool_ma
  group by 1
),

pool_month_alloc as (
  select
    pm.month_date,
    pm.pool_id,
    pm.pool_name,
    case
      when mt.month_ma_total > 0 then greatest(pm.nxm_reward_3m_moving_avg, 0) / mt.month_ma_total
      else 0
    end as pool_alloc_weight
  from pool_ma pm
  join month_totals mt on mt.month_date = pm.month_date
),

pool_stake_shares as (
  select
    pool_id,
    staker,
    nxm_active_stake,
    sum(nxm_active_stake) over (partition by pool_id) as pool_active_total,
    case
      when sum(nxm_active_stake) over (partition by pool_id) > 0 then nxm_active_stake / sum(nxm_active_stake) over (partition by pool_id)
      else 0
    end as staker_share_in_pool
  from active
),

staker_month_weight as (
  select
    pma.month_date,
    pma.pool_id,
    pma.pool_name,
    ps.staker,
    ps.nxm_active_stake,
    pma.pool_alloc_weight * ps.staker_share_in_pool as staker_month_weight
  from pool_month_alloc pma
  join pool_stake_shares ps on ps.pool_id = pma.pool_id
),

ramm_by_month as (
  select
    block_month as month_date,
    coalesce(bv_diff_nxm, 0) as bv_diff_nxm
  from ramm_added
)

select
  smw.month_date,
  smw.pool_id,
  smw.pool_name,
  smw.staker,
  smw.nxm_active_stake,
  smw.staker_month_weight * rbm.bv_diff_nxm as additional_nxm_from_ramm,
  smw.staker_month_weight
from staker_month_weight smw
left join ramm_by_month rbm on rbm.month_date = smw.month_date
order by smw.month_date, smw.pool_id, smw.staker
