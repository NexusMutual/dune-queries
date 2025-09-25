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
  from filtered_daily_staking_sim s
    inner join params p on true
  where s.date between p.start_date and p.end_date
),

factors as (
  select
    date,
    pool_id,
    pool_name,
    coalesce(1 + apy/36500.0, 1.0) as daily_factor
  from selected
),

running as (
  select
    date,
    pool_id,
    pool_name,
    exp(sum(ln(greatest(daily_factor, 1e-12))) over (partition by pool_id order by date rows between unbounded preceding and current row)) as cum_factor
  from factors
)

select
  r.date,
  r.pool_id,
  r.pool_name,
  p.stake_amount * r.cum_factor as balance,
  p.stake_amount * r.cum_factor - p.stake_amount as accrued_rewards
from running r cross join params p
order by 1, 2
