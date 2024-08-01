with

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    payment_asset,
    usd_cover_amount
  from query_3950051 -- opencover - quote base
)

select
  date_trunc('week', cover_start_date) as period_date,
  payment_asset,
  count(distinct cover_id) as cover_sold,
  sum(usd_cover_amount) as usd_cover_amount
from covers
group by 1, 2
order by 1, 2
