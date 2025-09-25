with

params as (
  select
    cast({{ start_date }} as date) as start_date,
    cast({{ end_date }} as date) as end_date
),

selected as (
  select
    s.date,
    s.pool_id,
    s.baseline_apy,
    s.baseline_apy_7d_ma,
    s.baseline_apy_30d_ma,
    s.baseline_apy_91d_ma,
    s.apy,
    s.apy_7d_ma,
    s.apy_30d_ma,
    s.apy_91d_ma
  from filtered_daily_staking_sim s
    inner join params p on true
  where s.date between p.start_date and p.end_date
)

select
  date,
  pool_id,
  baseline_apy,
  apy as test_apy,
  baseline_apy_7d_ma,
  apy_7d_ma as test_apy_7d_ma,
  baseline_apy_30d_ma,
  apy_30d_ma as test_apy_30d_ma,
  baseline_apy_91d_ma,
  apy_91d_ma as test_apy_91d_ma,
  abs(apy - baseline_apy) as apy_delta,
  abs(apy_30d_ma - baseline_apy_30d_ma) as apy_30d_delta
from selected
order by 1,2
