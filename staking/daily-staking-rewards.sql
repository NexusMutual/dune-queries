with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
)

select
  r.block_date,
  r.pool_id,
  spn.pool_name,
  r.reward_total,
  r.pool_date_rn
from query_4068272 r -- daily staking rewards base query
  left join staking_pool_names spn on r.pool_id = spn.pool_id
order by r.pool_id, r.block_date
