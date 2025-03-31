with renewals as (
  select
    cover_owner,
    count(cover_id) as total_covers,
    min(cover_start_date) as first_cover_date,
    max(cover_end_date) as last_cover_date,
    date_diff('month', min(cover_start_date), max(cover_end_date)) as coverage_months
  from query_4599092
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
  round(100.0 * renewal_cover_buyers / total_cover_buyers, 2) as renewal_rate_percentage,
  avg_renewal_months
from customer_counts
