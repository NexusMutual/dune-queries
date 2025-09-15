with

cover_sales_per_owner as (
  select *
  from query_5779616 -- bd cover owners - base root
  where last_cover_buy >= now() - interval '12' month
    and cover_owner <> 0x181aea6936b407514ebfc0754a37704eb8d98f91
),

bins as (
  select
    if(floor(log(2, product_sold)) < 0, -1, floor(log(2, product_sold))) as bin_floor,
    count(distinct cover_owner) as cover_owner_count
  from cover_sales_per_owner
  group by 1
)

select
  bin_floor,
  format_number(power(2, bin_floor)) || ' - ' || format_number(power(2, bin_floor + 1)) as bin_range,
  cover_owner_count
from bins
order by 1
