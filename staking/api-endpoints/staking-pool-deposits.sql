select
  block_date,
  pool_id,
  total_staked_nxm
--from query_4065286 -- staked nxm per pool - base
from nexusmutual_ethereum.staked_per_pool
order by pool_id, block_date
