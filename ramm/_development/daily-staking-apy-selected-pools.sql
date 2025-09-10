select
  block_date,
  pool_id,
  pool_name,
  total_staked_nxm,
  reward_total,
  apy,
  apy_7d_ma,
  apy_30d_ma,
  apy_91d_ma
from query_4068509 -- daily staking apy
where pool_id in (2, 22)
  and block_date >= timestamp '2024-01-01'
order by 1, 2
