select
  month_date,
  pool_id,
  pool_name,
  nxm_reward_total,
  eth_reward_total,
  usd_reward_total,
  avg(nxm_reward_total) over (partition by pool_id order by month_date rows between 2 preceding and current row) as nxm_reward_3m_moving_avg,
  avg(eth_reward_total) over (partition by pool_id order by month_date rows between 2 preceding and current row) as eth_reward_3m_moving_avg,
  avg(usd_reward_total) over (partition by pool_id order by month_date rows between 2 preceding and current row) as usd_reward_3m_moving_avg
from query_4248623 -- staking rewards monthly
where month_date >= timestamp '2024-01-01'
  and pool_id in (2, 22)
order by 1, 2
