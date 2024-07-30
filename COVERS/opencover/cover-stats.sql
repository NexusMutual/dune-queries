with

quotes as (
  select
    cover_id,
    quote_submitted_block_time,
    quote_submitted_sender,
    usd_cover_amount,
    usd_premium_amount,
    row_number() over (partition by quote_submitted_sender order by quote_submitted_block_time) as first_seen,
    if(quote_submitted_block_time >= now() - interval '7' day, true, false) as is_within_period
  from query_3950051 -- opencover - quote base
)

select
  count(*) as cover_sold,
  count(distinct quote_submitted_sender) as unique_cover_owners,
  sum(usd_cover_amount) as usd_cover_total,
  sum(usd_premium_amount) as usd_premium_total,
  coalesce(sum(usd_cover_amount) filter (where is_within_period), 0) as usd_cover_7day,
  coalesce(sum(usd_premium_amount) filter (where is_within_period), 0) as usd_premium_7day
from quotes
