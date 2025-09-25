with

params as (
  select
    cast({{ stake_amount }} as double) as stake_amount,
    cast({{ horizon_days | default(30) }} as integer) as horizon_days,
    cast({{ as_of_date }} as date) as as_of_date,
    cast({{ lookback_days | default(180) }} as integer) as lookback_days,
    cast({{ paths | default(50) }} as integer) as paths
),

hist as (
  select
    s.pool_id,
    s.pool_name,
    s.apy
  from filtered_daily_staking s
    inner join params p on true
  where s.date > p.as_of_date - interval '1' day * p.lookback_days
    and s.date <= p.as_of_date
),

pools as (
  select
    pool_id,
    pool_name,
    array_agg(apy) as apy_values,
    array_length(array_agg(apy)) as apy_count
  from hist
  group by 1,2
),

sim as (
  select
    pl.pool_id,
    pl.pool_name,
    u.path_id,
    exp(sum(ln(1 + array_extract(
                  pl.apy_values,
                  1 + cast(floor(random() * pl.apy_count) as int)
                )/36500.0))) as end_factor
  from pools pl
    inner join params p on true
    cross join generate_series(1, p.paths) as u(path_id)
    cross join generate_series(1, p.horizon_days) as v(t)
  group by 1,2,3
)

select
  s.pool_id,
  s.pool_name,
  p.stake_amount,
  approx_quantile(s.end_factor, 0.10) * p.stake_amount as final_p10,
  approx_quantile(s.end_factor, 0.50) * p.stake_amount as final_p50,
  approx_quantile(s.end_factor, 0.90) * p.stake_amount as final_p90
from sim s
  inner join params p on true
group by 1,2,3
order by 1
