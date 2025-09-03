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
)

select
  month_date,
  pool_id,
  pool_name,
  nxm_reward_total,
  eth_reward_total,
  usd_reward_total
from staking_rewards
union all
select
  block_month,
  null as pool_id,
  'RAMM' as pool_name,
  bv_diff_nxm,
  bv_diff_eth,
  bv_diff_usd
from ramm_added
order by 1, 2
