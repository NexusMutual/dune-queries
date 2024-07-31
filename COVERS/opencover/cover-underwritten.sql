with

covers as (
  select
    blockchain,
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_asset,
    usd_cover_amount,
    usd_premium_amount
  from query_3950051 -- opencover - quote base
)

select
  cover_asset,
  count(distinct cover_id) as cover_sold,
  sum(usd_cover_amount) as usd_cover_amount,
  sum(usd_premium_amount) as usd_premium_amount
from covers
group by 1
order by 1
