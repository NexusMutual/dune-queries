with

covers as (
  select
    blockchain,
    cover_id,
    cover_start_date,
    cover_end_date,
    date_diff('day', cover_start_date, cover_end_date) as cover_duration,
    usd_cover_amount,
    usd_premium_amount
  from query_3950051 -- opencover - quote base
)

select
  cover_duration,
  count(distinct cover_id) as cover_sold,
  sum(usd_cover_amount) as usd_cover_amount,
  sum(usd_premium_amount) as usd_premium_amount
from covers
group by 1
