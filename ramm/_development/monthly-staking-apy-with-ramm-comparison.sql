with

staking_pool_names (pool_id, pool_name) as (
  values
    (2, 'Hugh'),
    (22, 'BND')
),

staking_apy_current_state as (
  select
    block_date,
    pool_id,
    apy_30d_ma
  from query_5729587 -- daily staking apy (2+22)
),

staking_apy_with_ramm as (
  select
    month_date,
    pool_id,
    apy
  --from query_5733434 -- monthly staking apy (2+22)
  from dune.nexus_mutual.result_monthly_staking_apy_2_22
),

daily_as_month_end as (
  select
    cast(date_trunc('month', block_date) as date) as month_date,
    pool_id,
    max(block_date) as month_end_date,
    max_by(apy_30d_ma, block_date) as apy_30d_ma_month_end
  from staking_apy_current_state
  group by 1, 2
),

staking_apy_combined as (
  select
    month_date,
    pool_id,
    'current' as series,
    apy_30d_ma_month_end as apy
  from daily_as_month_end
  union all
  select
    month_date,
    pool_id,
    'with ramm' as series,
    apy
  from staking_apy_with_ramm
)

select
  sc.month_date,
  concat(spn.pool_name, ' (', sc.series, ')') as series,
  sc.apy
from staking_apy_combined sc
  inner join staking_pool_names spn on sc.pool_id = spn.pool_id
order by 1, 2
