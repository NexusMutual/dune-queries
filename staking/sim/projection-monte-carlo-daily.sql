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
  from filtered_daily_staking_sim s
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

steps as (
  select
    pl.pool_id,
    pl.pool_name,
    u.path_id,
    d.day_num,
    array_extract(
      pl.apy_values,
      1 + cast(floor(random() * pl.apy_count) as int)
    ) as apy_draw
  from pools pl
    inner join params p on true
    cross join generate_series(1, p.paths) as u(path_id)
    cross join generate_series(1, p.horizon_days) as d(day_num)
),

path_prod as (
  select
    pool_id,
    pool_name,
    path_id,
    day_num,
    exp(sum(ln(1 + coalesce(apy_draw, 0)/36500.0)) over (
      partition by pool_id, pool_name, path_id
      order by day_num
      rows between unbounded preceding and current row
    )) as cum_factor
  from steps
),

daily_pct as (
  select
    pool_id,
    pool_name,
    day_num,
    approx_quantile(cum_factor, 0.10) as factor_p10,
    approx_quantile(cum_factor, 0.50) as factor_p50,
    approx_quantile(cum_factor, 0.90) as factor_p90
  from path_prod
  group by 1,2,3
)

select
  d.pool_id,
  d.pool_name,
  current_date + (interval 1 day) * d.day_num as projection_date,
  d.day_num as horizon_day,
  p.stake_amount as stake,
  p.stake_amount * d.factor_p10 as bal_p10,
  p.stake_amount * d.factor_p50 as bal_p50,
  p.stake_amount * d.factor_p90 as bal_p90,
  p.stake_amount * d.factor_p10 - p.stake_amount as rew_p10,
  p.stake_amount * d.factor_p50 - p.stake_amount as rew_p50,
  p.stake_amount * d.factor_p90 - p.stake_amount as rew_p90
from daily_pct d
  inner join params p on true
order by 1, 3
