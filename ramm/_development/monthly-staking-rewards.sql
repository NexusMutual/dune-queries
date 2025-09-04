select
  month_date,
  pool_id,
  pool_name,
  nxm_reward_total,
  eth_reward_total,
  usd_reward_total
from query_4248623 -- staking rewards monthly
where month_date >= timestamp '2024-01-01'
order by 1, 2
