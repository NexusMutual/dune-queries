with

params as (
  select
    cast({{ start_date }} as date) as start_date,
    cast({{ end_date }} as date) as end_date,
    cast({{ stake_amount }} as double) as stake_amount,
    {{ rate_col | default('APY') }} as rate_col
),

selected as (
  select
    s.date,
    s.pool_id,
    s.pool_name,
    case
      when p.rate_col = 'APY' then s.apy
      when p.rate_col = 'APY 7d MA' then s.apy_7d_ma
      when p.rate_col = 'APY 30d MA' then s.apy_30d_ma
      when p.rate_col = 'APY 91d MA' then s.apy_91d_ma
      else s.apy
    end as apy
  from filtered_daily_staking s
    inner join params p on true
  where s.date between p.start_date and p.end_date
),

agg as (
  select
    pool_id,
    pool_name,
    sum(ln(coalesce(1 + apy/36500.0, 1.0))) as sum_log_factor
  from selected
  group by 1,2
)

select
  a.pool_id,
  a.pool_name,
  p.stake_amount,
  exp(a.sum_log_factor) * p.stake_amount - p.stake_amount as total_rewards,
  exp(a.sum_log_factor) * p.stake_amount as final_balance
from agg a
  inner join params p on true
order by 1
