with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
)

select
  s.block_date,
  s.pool_id,
  spn.pool_name,
  s.total_staked_nxm,
  s.pool_date_rn
from query_4065286 s -- staked nxm base query
  left join staking_pool_names spn on s.pool_id = spn.pool_id
order by s.pool_id, s.block_date
