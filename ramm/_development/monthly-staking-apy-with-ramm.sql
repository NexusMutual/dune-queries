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
),

joined as (
  select
    s.month_date,
    s.pool_id,
    spn.pool_name,
    s.total_staked_nxm,
    r.nxm_reward_total,
    r.nxm_reward_total / nullif(s.total_staked_nxm, 0) as monthly_rate
  from monthly_stake s
    inner join query_5724805 r
      on s.pool_id = r.pool_id and s.month_date = r.month_date
    left join staking_pool_names spn on s.pool_id = spn.pool_id
  where s.month_date >= timestamp '2024-01-01'
)

select
  month_date,
  pool_id,
  pool_name,
  total_staked_nxm,
  nxm_reward_total,
  nxm_reward_3m_moving_avg,
  monthly_rate * 1200.0 as apy,
  avg(monthly_rate) over (
    partition by pool_id
    order by month_date
    rows between 2 preceding and current row
  ) * 1200.0 as apy_3m_moving_avg
from joined
order by pool_id, month_date
