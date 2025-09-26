with

params as (
  select
    cast({{ stake_amount }} as double) as stake_amount,
    cast({{ horizon_days }} as integer) as horizon_days,
    cast({{ as_of_date }} as date) as as_of_date,
    {{ rate_col | default('APY 30d MA') }} as rate_col
),

lookback as (
  select
    s.pool_id,
    s.pool_name,
    avg(case
      when p.rate_col = 'APY' then s.apy
      when p.rate_col = 'APY 7d MA' then s.apy_7d_ma
      when p.rate_col = 'APY 30d MA' then s.apy_30d_ma
      when p.rate_col = 'APY 91d MA' then s.apy_91d_ma
      else s.apy
    end) as avg_apy
  from filtered_daily_staking_sim s
    inner join params p on true
  where s.date > p.as_of_date - interval '30' day
    and s.date <= p.as_of_date
  group by 1,2
)

select
  l.pool_id,
  l.pool_name,
  p.horizon_days,
  p.as_of_date,
  p.stake_amount,
  pow(1 + coalesce(l.avg_apy, 0)/36500.0, p.horizon_days) * p.stake_amount - p.stake_amount as projected_rewards,
  pow(1 + coalesce(l.avg_apy, 0)/36500.0, p.horizon_days) * p.stake_amount as projected_final_balance
from lookback l
  inner join params p on true
order by 1
