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
  r.reward_total,
  r.reward_total / nullif(s.total_staked_nxm, 0) * 36500.0 as apy
--from query_4065286 s -- staked nxm base query (uses staking pools - spell de-duped)
from nexusmutual_ethereum.staked_per_pool s
  inner join query_4068272 r -- daily staking rewards base query
    on s.pool_id = r.pool_id and s.block_date = r.block_date
  left join staking_pool_names spn on s.pool_id = spn.pool_id
--where s.block_date >= date_add('month', -3, current_date)
order by s.pool_id, s.block_date
