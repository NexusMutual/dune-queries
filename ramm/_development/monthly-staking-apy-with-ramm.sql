with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

monthly_stake as (
  select
    date_trunc('month', block_date) as month_date,
    pool_id,
    max(block_date) as block_date,
    max_by(total_staked_nxm, block_date) as total_staked_nxm -- at end of month
  from dune.nexus_mutual.result_staked_nxm_per_pool
  group by 1, 2
)

select
  s.month_date,
  s.pool_id,
  spn.pool_name,
  s.total_staked_nxm,
  r.nxm_reward_total,
  r.nxm_reward_total / nullif(s.total_staked_nxm, 0) * 1200.0 as apy
from monthly_stake s
  inner join query_5724805 r -- monthly staking rewards with split value from RAMM
    on s.pool_id = r.pool_id and s.month_date = r.month_date
  left join staking_pool_names spn on s.pool_id = spn.pool_id
where s.block_date >= timestamp '2024-01-01'
order by s.pool_id, s.month_date
