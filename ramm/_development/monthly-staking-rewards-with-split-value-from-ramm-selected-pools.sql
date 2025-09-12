select
  month_date,
  pool_id,
  pool_name,
  nxm_reward_total,
  eth_reward_total,
  usd_reward_total,
  nxm_reward_3m_moving_avg,
  eth_reward_3m_moving_avg,
  usd_reward_3m_moving_avg
from query_5724805 -- monthly staking rewards with split value from ramm
where pool_id in (2, 22)
order by 1, 2
