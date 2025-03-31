with

covers as (
  select distinct cover_id, cover_owner, cover_start_date, cover_end_date, commission_destination
  from query_4599092
),

renewals as (
  select
    cover_owner,
    count(cover_id) as total_covers,
    min(cover_start_date) as first_cover_date,
    max(cover_end_date) as last_cover_date,
    date_diff('month', min(cover_start_date), max(cover_end_date)) as coverage_months
  from covers
  where commission_destination not in (
      -- OpenCover:
      0xe4994082a0e7f38b565e6c5f4afd608de5eddfbb,
      0x40329f3e27dd3fe228799b4a665f6f104c2ab6b4,
      0x5f2b6e70aa6a217e9ecd1ed7d0f8f38ce9a348a2,
      0x02bdacb2c3baa8a12d3957f3bd8637d6d2b35f10
    )
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
