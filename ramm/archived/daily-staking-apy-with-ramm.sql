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
  r.nxm_reward_total,
  r.nxm_reward_total / nullif(s.total_staked_nxm, 0) * 36500.0 as apy,
  avg(
    (r.nxm_reward_total / nullif(s.total_staked_nxm, 0)) * 36500.0
  ) over (
    partition by s.pool_id
    order by s.block_date
    rows between 6 preceding and current row
  ) as apy_7d_ma,
  avg(
    (r.nxm_reward_total / nullif(s.total_staked_nxm, 0)) * 36500.0
  ) over (
    partition by s.pool_id
    order by s.block_date
    rows between 29 preceding and current row
  ) as apy_30d_ma
from dune.nexus_mutual.result_staked_nxm_per_pool s
  inner join query_5730117 r -- daily staking rewards with ramm
    on s.pool_id = r.pool_id and s.block_date = r.block_date
  left join staking_pool_names spn on s.pool_id = spn.pool_id
where s.block_date >= timestamp '2024-01-01'
order by s.pool_id, s.block_date
