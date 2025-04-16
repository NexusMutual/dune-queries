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

gap_distribution as (
  select
    count(*) filter (where days_gap between 1 and 7) * 1.0000 / count(*) as pct_renewals_with_gap_1_7d,
    count(*) filter (where days_gap between 8 and 30) * 1.0000 / count(*) as pct_renewals_with_gap_8_30d,
    count(*) filter (where days_gap between 31 and 90) * 1.0000 / count(*) as pct_renewals_with_gap_31_90d,
    count(*) filter (where days_gap > 90) * 1.0000 / count(*) as pct_renewals_with_gap_gt_90d,
    stddev_samp(days_gap) as avg_gap_stddev
  from covers_with_prev
  where days_gap > 0
),

gap_summary as (
  select
    approx_percentile(days_gap * 1.0000, 0.5) as global_median_renewal_gap
  from covers_with_prev
  where days_gap > 0
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
    max(streak_months) as longest_continuous_coverage_months,
    count(distinct streak_id) as streak_count
  from streak_durations
  group by 1
),

renewal_gaps as (
  select
    cover_owner,
    avg(days_gap) as avg_days_between_covers,
    approx_percentile(days_gap * 1.0000, 0.5) as median_days_between_covers,
    max(days_gap) as max_days_gap,
    stddev_samp(days_gap) as gap_stddev,
    count(*) as gap_count,
    count(case when days_gap = 0 then 1 end) as zero_gap_count,
    count(case when days_gap > 0 then 1 end) as nonzero_gap_count,
    max(days_gap) as final_gap_days
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
    ls.streak_count,
    rg.avg_days_between_covers,
    rg.median_days_between_covers,
    rg.max_days_gap,
    rg.final_gap_days,
    rg.gap_stddev,
    rg.gap_count,
    rg.zero_gap_count,
    rg.nonzero_gap_count
  from renewals r
    left join longest_streaks ls on r.cover_owner = ls.cover_owner
    left join renewal_gaps rg on r.cover_owner = rg.cover_owner
),

customer_stats as (
  select
    count(distinct cover_owner) as total_cover_buyers,
    count(case when total_covers > 1 then cover_owner end) as renewal_cover_buyers,
    avg(case when total_covers > 1 then coverage_lifetime_months end) as avg_coverage_lifetime_months,
    avg(total_covers * 1.0000) as avg_covers_per_buyer,
    approx_percentile(total_covers * 1.0000, 0.5) as median_covers_per_buyer,
    max(total_covers) as max_covers_by_single_buyer,
    max(longest_continuous_coverage_months) as longest_continuous_coverage_months,
    avg(streak_count * 1.0000) as avg_streaks_per_user,
    count(case when coverage_lifetime_months >= 3 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_3plus_months,
    count(case when coverage_lifetime_months >= 12 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_12plus_months,
    avg(avg_days_between_covers) as avg_days_between_covers,
    max(median_days_between_covers) as median_days_between_covers,
    count(case when max_days_gap <= 30 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_max_gap_30d,
    count(case when max_days_gap <= 90 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_max_gap_90d,
    count(case when zero_gap_count > 0 and nonzero_gap_count = 0 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_all_zero_gaps,
    count(case when nonzero_gap_count > 0 and zero_gap_count = 0 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_all_nonzero_gaps,
    count(case when nonzero_gap_count > 0 and zero_gap_count > 0 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_mixed_gaps,
    avg(gap_count * 1.0000) as avg_gap_count_per_buyer,
    avg(final_gap_days) as avg_final_gap_days,
    count(case when final_gap_days > 30 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_final_gap_gt_30d,
    count(case when zero_gap_count = 0 then cover_owner end) * 1.0000 / count(distinct cover_owner) as pct_buyers_with_gap_but_no_overlap
  from renewals_ext
)

select
  metric.item,
  metric.value
from (
  select
    map(
      array[
        '-- basic stats',
        'total cover buyers',
        'renewal buyers',
        'renewal rate (%)',

        '-- coverage lifetime',
        'avg coverage lifetime (months)',
        'avg covers per buyer',
        'median covers per buyer',
        'max covers by one buyer',
        'longest continuous coverage (months)',
        'avg coverage streaks per buyer',

        '-- coverage thresholds',
        'buyers with ≥3 months coverage (%)',
        'buyers with ≥12 months coverage (%)',

        '-- renewal timing',
        'avg renewal gap (days)',
        'median renewal gap (days)',
        'global median renewal gap (days)',
        'renewal gap stddev',
        'avg final gap (days)',
        'avg renewal count per buyer',

        '-- renewal delay buckets',
        'renewals with 1–7d gap (%)',
        'renewals with 8–30d gap (%)',
        'renewals with 31–90d gap (%)',
        'renewals with >90d gap (%)',

        '-- buyer behavior',
        'buyers with all gaps ≤30d (%)',
        'buyers with all gaps ≤90d (%)',
        'buyers with only instant renewals (%)',
        'buyers with only delayed renewals (%)',
        'buyers with mixed renewals (%)',
        'buyers with final gap >30d (%)',
        'buyers with no overlap ever (%)'
      ],
      array[
        cast(null as varchar),
        cast(total_cover_buyers as varchar),
        cast(renewal_cover_buyers as varchar),
        cast(renewal_cover_buyers * 1.0000 / total_cover_buyers as varchar),

        cast(null as varchar),
        cast(avg_coverage_lifetime_months as varchar),
        cast(avg_covers_per_buyer as varchar),
        cast(median_covers_per_buyer as varchar),
        cast(max_covers_by_single_buyer as varchar),
        cast(longest_continuous_coverage_months as varchar),
        cast(avg_streaks_per_user as varchar),

        cast(null as varchar),
        cast(pct_buyers_with_3plus_months as varchar),
        cast(pct_buyers_with_12plus_months as varchar),

        cast(null as varchar),
        cast(avg_days_between_covers as varchar),
        cast(median_days_between_covers as varchar),
        cast(gs.global_median_renewal_gap as varchar),
        cast(gd.avg_gap_stddev as varchar),
        cast(avg_final_gap_days as varchar),
        cast(avg_gap_count_per_buyer as varchar),

        cast(null as varchar),
        cast(gd.pct_renewals_with_gap_1_7d as varchar),
        cast(gd.pct_renewals_with_gap_8_30d as varchar),
        cast(gd.pct_renewals_with_gap_31_90d as varchar),
        cast(gd.pct_renewals_with_gap_gt_90d as varchar),

        cast(null as varchar),
        cast(pct_buyers_with_max_gap_30d as varchar),
        cast(pct_buyers_with_max_gap_90d as varchar),
        cast(pct_buyers_with_all_zero_gaps as varchar),
        cast(pct_buyers_with_all_nonzero_gaps as varchar),
        cast(pct_buyers_with_mixed_gaps as varchar),
        cast(pct_buyers_with_final_gap_gt_30d as varchar),
        cast(pct_buyers_with_gap_but_no_overlap as varchar)
      ]
    ) as metrics
  from customer_stats cs
    cross join gap_summary gs
    cross join gap_distribution gd
) t
  cross join unnest(map_keys(metrics), map_values(metrics)) as metric(item, value)
