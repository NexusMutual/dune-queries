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
    s.pool_name,
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
),

-- first available on/after start_date per pool
at_start as (
  select
    pool_id,
    pool_name,
    baseline_apy as baseline_apy_start,
    apy as apy_start
  from (
    select
      *,
      row_number() over (partition by pool_id order by date asc) as rn
    from selected
      inner join params p on true
    where date >= p.start_date
  )
  where rn = 1
),

-- last available on/before end_date per pool
at_end as (
  select
    pool_id,
    pool_name,
    baseline_apy as baseline_apy_end,
    apy as apy_end
  from (
    select
      *,
      row_number() over (partition by pool_id order by date desc) as rn
    from selected
      inner join params p on true
    where date <= p.end_date
  )
  where rn = 1
),

period_ma as (
  select
    pool_id,
    pool_name,
    avg(baseline_apy_7d_ma) as baseline_apy_7d_ma_avg,
    avg(apy_7d_ma) as apy_7d_ma_avg,
    avg(baseline_apy_30d_ma) as baseline_apy_30d_ma_avg,
    avg(apy_30d_ma) as apy_30d_ma_avg,
    avg(baseline_apy_91d_ma) as baseline_apy_91d_ma_avg,
    avg(apy_91d_ma) as apy_91d_ma_avg
  from selected
  group by 1,2
)

select
  coalesce(s.pool_id, e.pool_id, m.pool_id) as pool_id,
  coalesce(s.pool_name, e.pool_name, m.pool_name) as pool_name,
  printf('%.2f%% -> %.2f%%', s.baseline_apy_start, s.apy_start) as apy_at_start,
  printf('%.2f%% -> %.2f%%', e.baseline_apy_end, e.apy_end) as apy_at_end,
  printf('%.2f%% -> %.2f%%', m.baseline_apy_7d_ma_avg, m.apy_7d_ma_avg) as apy_7d_ma_over_period,
  printf('%.2f%% -> %.2f%%', m.baseline_apy_30d_ma_avg, m.apy_30d_ma_avg) as apy_30d_ma_over_period,
  printf('%.2f%% -> %.2f%%', m.baseline_apy_91d_ma_avg, m.apy_91d_ma_avg) as apy_91d_ma_over_period
from at_start s
  full outer join at_end e on s.pool_id = e.pool_id
  full outer join period_ma m on coalesce(s.pool_id, e.pool_id) = m.pool_id
order by 1
