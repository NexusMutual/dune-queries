with

covers as (
  select
    blockchain,
    cover_id,
    cover_start_date,
    cover_end_date,
    payment_asset,
    usd_premium_amount
  from query_3950051 -- opencover - quote base
)

select
  payment_asset,
  count(distinct cover_id) as premium_collected,
  sum(usd_premium_amount) as usd_premium_amount
from covers
group by 1
order by 1
