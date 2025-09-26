with

params as (
  select
    cast({{ stake_amount }} as double) as stake_amount,
    cast({{ horizon_days }} as integer) as horizon_days,
    cast({{ as_of_date }} as date) as as_of_date,
    cast({{ lookback_days | default(180) }} as integer) as lookback_days,
    {{ rate_col | default('APY') }} as rate_col
),

hist as (
  select
    s.pool_id,
    s.pool_name,
    case
      when p.rate_col = 'APY' then s.apy
      when p.rate_col = 'APY 7d MA' then s.apy_7d_ma
      when p.rate_col = 'APY 30d MA' then s.apy_30d_ma
      when p.rate_col = 'APY 91d MA' then s.apy_91d_ma
      else s.apy
    end as apy
  from filtered_daily_staking_sim s
    inner join params p on true
  where s.date > p.as_of_date - interval '1' day * p.lookback_days
    and s.date <= p.as_of_date
),

q as (
  select
    pool_id,
    pool_name,
    approx_quantile(apy, 0.10) as apy_p10,
    approx_quantile(apy, 0.50) as apy_p50,
    approx_quantile(apy, 0.90) as apy_p90
  from hist
  group by 1,2
)

select
  q.pool_id,
  q.pool_name,
  p.lookback_days,
  p.horizon_days,
  p.as_of_date,
  p.stake_amount,
  pow(1 + coalesce(q.apy_p10, 0)/36500.0, p.horizon_days) * p.stake_amount as final_low,
  pow(1 + coalesce(q.apy_p50, 0)/36500.0, p.horizon_days) * p.stake_amount as final_base,
  pow(1 + coalesce(q.apy_p90, 0)/36500.0, p.horizon_days) * p.stake_amount as final_high
from q
  inner join params p on true
order by 1
