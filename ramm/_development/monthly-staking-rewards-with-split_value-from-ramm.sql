with

staking_rewards as (
  select
    month_date,
    pool_id,
    pool_name,
    nxm_reward_total,
    eth_reward_total,
    usd_reward_total
  from query_4248623 -- staking rewards monthly
  where month_date >= timestamp '2024-01-01'
),

ramm_added as (
  select
    block_month,
    bv_diff_eth,
    bv_diff_nxm,
    bv_diff_usd
  from query_5232824 -- ramm monthly value accrual
),

monthly_denoms as (
  select
    month_date,
    sum(case when usd_reward_total > 0 then usd_reward_total else 0 end) as denom_usd
  from staking_rewards
  group by 1
),

pool_weights as (
  select
    sr.month_date,
    sr.pool_id,
    sr.pool_name,
    case
      when sr.usd_reward_total > 0 and md.denom_usd > 0 then sr.usd_reward_total / md.denom_usd
      else 0
    end as usd_weight
  from staking_rewards sr
    inner join monthly_denoms md on md.month_date = sr.month_date
),

ramm_alloc as (
  select
    pw.month_date,
    pw.pool_id,
    pw.usd_weight * ra.bv_diff_nxm as add_nxm,
    pw.usd_weight * ra.bv_diff_eth as add_eth,
    pw.usd_weight * ra.bv_diff_usd as add_usd
  from pool_weights pw
    left join ramm_added ra on ra.block_month = pw.month_date
)

select
  sr.month_date,
  sr.pool_id,
  sr.pool_name,
  sr.nxm_reward_total + coalesce(ra.add_nxm, 0) as nxm_reward_total,
  sr.eth_reward_total + coalesce(ra.add_eth, 0) as eth_reward_total,
  sr.usd_reward_total + coalesce(ra.add_usd, 0) as usd_reward_total
from staking_rewards sr
  left join ramm_alloc ra on ra.month_date = sr.month_date and ra.pool_id = sr.pool_id
order by 1, 2
