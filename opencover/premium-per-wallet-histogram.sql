with

cover_sales_per_owner as (
  select *
  from query_4086950 -- cover owner overview
  where last_cover_buy >= now() - interval '12' month
),

bins as (
  select
    if(floor(log(3, usd_premium_amount)) < 0, -1, floor(log(3, usd_premium_amount))) as bin_floor,
    count(distinct cover_owner) as cover_owner_count
  from cover_sales_per_owner
  group by 1
)

select
  bin_floor,
  format_number(power(3, bin_floor)) || ' - ' || format_number(power(3, bin_floor + 1)) as bin_range,
  cover_owner_count
from bins
order by 1
