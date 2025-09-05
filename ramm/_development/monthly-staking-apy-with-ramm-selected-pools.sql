select
  month_date,
  pool_id,
  pool_name,
  total_staked_nxm,
  nxm_reward_total,
  apy
from query_5733364 -- monthly staking apy with ramm
where pool_id in (2, 22)
  and month_date >= timestamp '2024-01-01'
order by 1, 2
