with

covers as (
  select distinct cover_id, cover_owner, cover_start_date, cover_end_date
  from query_4599092
  where commission_destination not in (
    -- OpenCover:
    0xe4994082a0e7f38b565e6c5f4afd608de5eddfbb,
    0x40329f3e27dd3fe228799b4a665f6f104c2ab6b4,
    0x5f2b6e70aa6a217e9ecd1ed7d0f8f38ce9a348a2,
    0x02bdacb2c3baa8a12d3957f3bd8637d6d2b35f10
  )
),

ordered_covers as (
  select
    cover_owner,
    cover_start_date,
    cover_end_date,
    row_number() over (partition by cover_owner order by cover_start_date) as rn
  from covers
),

covers_with_prev as (
  select
    cover_owner,
    cover_start_date,
    cover_end_date,
    lag(cover_end_date) over (partition by cover_owner order by cover_start_date) as prev_end_date,
    greatest(0, date_diff('day', lag(cover_end_date) over (partition by cover_owner order by cover_start_date), cover_start_date)) as days_gap
  from ordered_covers
),

marked_gaps as (
  select
    *,
    case when prev_end_date is null or cover_start_date > prev_end_date then 1 else 0 end as new_streak_flag
  from covers_with_prev
),

streaks as (
  select
    *,
    sum(new_streak_flag) over (partition by cover_owner order by cover_start_date) as streak_id
  from marked_gaps
),

streak_durations as (
  select
    cover_owner,
    streak_id,
    min(cover_start_date) as streak_start,
    max(cover_end_date) as streak_end,
    date_diff('month', min(cover_start_date), max(cover_end_date)) as streak_months
  from streaks
  group by 1, 2
),

longest_streaks as (
  select
    cover_owner,
    max(streak_months) as longest_continuous_coverage_months
  from streak_durations
  group by 1
),

renewal_gaps as (
  select
    cover_owner,
    avg(days_gap) as avg_days_between_covers,
    approx_percentile(days_gap * 1.0, 0.5) as median_days_between_covers,
    max(days_gap) as max_days_gap
  from covers_with_prev
  where days_gap is not null
  group by 1
),

renewals as (
  select
    cover_owner,
    count(cover_id) as total_covers,
    min(cover_start_date) as first_cover_date,
    max(cover_end_date) as last_cover_date,
    date_diff('month', min(cover_start_date), max(cover_end_date)) as coverage_lifetime_months
  from covers
  group by 1
),

renewals_ext as (
  select
    r.cover_owner,
    r.total_covers,
    r.first_cover_date,
    r.last_cover_date,
    r.coverage_lifetime_months,
    ls.longest_continuous_coverage_months,
    rg.avg_days_between_covers,
    rg.median_days_between_covers,
    rg.max_days_gap
  from renewals r
    left join longest_streaks ls on r.cover_owner = ls.cover_owner
    left join renewal_gaps rg on r.cover_owner = rg.cover_owner
),

customer_stats as (
  select
    count(distinct cover_owner) as total_cover_buyers,
    count(case when total_covers > 1 then cover_owner end) as renewal_cover_buyers,
    avg(case when total_covers > 1 then coverage_lifetime_months end) as avg_coverage_lifetime_months,
    avg(total_covers * 1.0) as avg_covers_per_buyer,
    approx_percentile(total_covers * 1.0, 0.5) as median_covers_per_buyer,
    max(total_covers) as max_covers_by_single_buyer,
    max(longest_continuous_coverage_months) as longest_continuous_coverage_months,
    count(case when coverage_lifetime_months >= 3 then cover_owner end) * 1.0 / count(distinct cover_owner) as pct_buyers_with_3plus_months,
    count(case when coverage_lifetime_months >= 12 then cover_owner end) * 1.0 / count(distinct cover_owner) as pct_buyers_with_12plus_months,
    avg(avg_days_between_covers) as avg_days_between_covers,
    approx_percentile(median_days_between_covers * 1.0, 0.5) as median_days_between_covers,
    count(case when max_days_gap <= 30 then cover_owner end) * 1.0 / count(distinct cover_owner) as pct_buyers_with_max_gap_30d,
    count(case when max_days_gap <= 90 then cover_owner end) * 1.0 / count(distinct cover_owner) as pct_buyers_with_max_gap_90d
  from renewals_ext
)

select
  total_cover_buyers,
  renewal_cover_buyers,
  renewal_cover_buyers * 1.0 / total_cover_buyers as renewal_rate_percentage,
  avg_coverage_lifetime_months,
  avg_covers_per_buyer,
  median_covers_per_buyer,
  max_covers_by_single_buyer,
  longest_continuous_coverage_months,
  pct_buyers_with_3plus_months,
  pct_buyers_with_12plus_months,
  avg_days_between_covers,
  median_days_between_covers,
  pct_buyers_with_max_gap_30d,
  pct_buyers_with_max_gap_90d
from customer_stats
