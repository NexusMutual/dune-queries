with

cover_sales_per_owner as (
  select *
  from query_3913267 -- BD cover owners base
  where last_cover_buy >= now() - interval '12' month
    and cover_owner <> 0x181aea6936b407514ebfc0754a37704eb8d98f91
),

bins as (
  select
    floor(log(2, cover_sold)) as bin_floor,
    count(distinct cover_owner) as cover_owner_count
  from cover_sales_per_owner
  group by 1
)

select
  bin_floor,
  format_number(power(2, cast(bin_floor as integer))) || ' - ' || format_number(power(2, cast(bin_floor + 1 as integer))) as bin_range,
  cover_owner_count
from bins
order by 1
