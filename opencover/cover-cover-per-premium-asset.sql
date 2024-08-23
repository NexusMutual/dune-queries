with

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    payment_asset,
    usd_premium_amount
  from query_3950051 -- opencover - quote base
)

select
  date_trunc('{{period}}', cover_start_date) as period_date,
  payment_asset,
  sum(usd_premium_amount) as usd_premium_amount
from covers
where cover_start_date >= date_add('{{period}}', -1 * {{period length}}, current_date)
group by 1, 2
order by 1, 2
