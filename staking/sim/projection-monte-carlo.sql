with

params as (
  select
    cast({{ stake_amount }} as double) as stake_amount,
    cast({{ horizon_days }} as integer) as horizon_days,
    cast({{ as_of_date }} as date) as as_of_date,
    cast({{ lookback_days | default(365) }} as integer) as lookback_days,
    cast({{ paths | default(100) }} as integer) as paths
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

grid as (
  select
    h.pool_id,
    h.pool_name,
    path_id,
    t
  from hist h
    inner join params p on true
    cross join unnest(generate_series(1, p.paths)) as u(path_id)
    cross join unnest(generate_series(1, p.horizon_days)) as v(t)
),

sampled as (
  select
    g.pool_id,
    g.pool_name,
    g.path_id,
    g.t,
    h.apy,
    row_number() over (
      partition by g.pool_id, g.pool_name, g.path_id, g.t
      order by random()
    ) as rn
  from grid g
    inner join hist h
      on h.pool_id = g.pool_id
      and h.pool_name = g.pool_name
),
steps as (
  select
    pool_id,
    pool_name,
    path_id,
    t,
    apy
  from sampled
  where rn = 1
),

path_prod as (
  select
    pool_id,
    pool_name,
    path_id,
    t,
    exp(sum(ln(1 + coalesce(apy, 0)/36500.0)) over (
      partition by pool_id, pool_name, path_id
      order by t
      rows between unbounded preceding and current row
    )) as cum_factor
  from steps
),

finals as (
  select
    pool_id,
    pool_name,
    path_id,
    max(cum_factor) as end_factor
  from path_prod
  group by 1,2,3
)

select
  f.pool_id,
  f.pool_name,
  p.stake_amount,
  approx_quantile(f.end_factor, 0.10) * p.stake_amount as final_p10,
  approx_quantile(f.end_factor, 0.50) * p.stake_amount as final_p50,
  approx_quantile(f.end_factor, 0.90) * p.stake_amount as final_p90
from finals f
  inner join params p on true
group by 1,2,3
order by 1
