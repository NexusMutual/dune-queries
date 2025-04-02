with

covers as (
  select distinct cover_id, cover_owner, cover_start_date, cover_end_date
  from query_3950051 -- opencover - quote base
),

renewals as (
  select
    cover_owner,
    count(cover_id) as total_covers,
    min(cover_start_date) as first_cover_date,
    max(cover_end_date) as last_cover_date,
    date_diff('month', min(cover_start_date), max(cover_end_date)) as coverage_months
  from covers
  group by 1
),

customer_counts as (
  select
    count(distinct cover_owner) as total_cover_buyers,
    count(case when total_covers > 1 then cover_owner end) as renewal_cover_buyers,
    avg(case when total_covers > 1 then coverage_months end) as avg_renewal_months
  from renewals
)

select
  total_cover_buyers,
  renewal_cover_buyers,
  renewal_cover_buyers * 1.0000 / total_cover_buyers as renewal_rate_percentage,
  avg_renewal_months
from customer_counts
