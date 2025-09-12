with

staking_pool_names (pool_id, pool_name) as (
  values
    (2, 'Hugh'),
    (22, 'BND')
),

rewards_baseline as (
  select
    month_date,
    pool_id,
    nxm_reward_total,
    avg(nxm_reward_total) over (partition by pool_id order by month_date rows between 2 preceding and current row) as nxm_reward_3m_moving_avg
  from query_4248623 -- staking rewards monthly
  where month_date >= timestamp '2024-01-01'
    and pool_id in (2, 22)
),

rewards_with_ramm as (
  select
    month_date,
    pool_id,
    nxm_reward_total,
    nxm_reward_3m_moving_avg
  from query_5724805 -- monthly staking rewards with split value from ramm
  where month_date >= timestamp '2024-01-01'
    and pool_id in (2, 22)
),

combined as (
  select
    month_date,
    pool_id,
    'baseline' as series,
    nxm_reward_total,
    nxm_reward_3m_moving_avg
  from rewards_baseline
  union all
  select
    month_date,
    pool_id,
    'with ramm' as series,
    nxm_reward_total,
    nxm_reward_3m_moving_avg
  from rewards_with_ramm
)

select
  c.month_date,
  c.pool_id,
  concat(spn.pool_name, ' (', c.series, ')') as series,
  c.nxm_reward_total,
  c.nxm_reward_3m_moving_avg
from combined c
  inner join staking_pool_names spn on c.pool_id = spn.pool_id
order by 1, 2
