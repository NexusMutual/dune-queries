-- stake-share multiplier (lock boost) over time
with

staking_periods as (
  select * from (values
    (1, 91),
    (2, 182),
    (3, 273),
    (4, 364),
    (5, 455),
    (6, 546),
    (7, 637),
    (8, 728)
  ) as t(period, lock_days)
),

grid as (
  select p.period, p.lock_days, x as day_from_start
  from staking_periods p
    cross join unnest(sequence(0, p.lock_days)) as u(x)
),

calc as (
  select
    period,
    lock_days,
    day_from_start,
    1.0000000000 + 0.4000000000 * greatest(lock_days - day_from_start, 0) / 365.0000000000 as boost
  from grid
)

select
  period,
  lock_days,
  day_from_start,
  concat('p', cast(period as varchar), ' - ', cast(lock_days as varchar), 'd') as period_label,
  boost,
  (boost - 1.0) * 100.0 as boost_pct
from calc
order by period, day_from_start
