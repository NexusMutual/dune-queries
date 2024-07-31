with

covers as (
  select
    cover_id,
    quote_submitted_block_time,
    cover_owner,
    usd_cover_amount,
    usd_premium_amount,
    row_number() over (partition by cover_owner order by quote_submitted_block_time) as first_seen,
    if(quote_submitted_block_time >= now() - interval '7' day, true, false) as is_within_period
  from query_3950051 -- opencover - quote base
),

new_cover_buyers as (
  select distinct cover_id, cover_owner
  from covers
  where cover_owner in (
    select cover_owner from covers where is_within_period and first_seen = 1
  )
)

select
  count(*) as cover_sold,
  count(distinct cover_owner) as unique_cover_owners,
  sum(usd_cover_amount) as usd_cover_total,
  sum(usd_premium_amount) as usd_premium_total,
  coalesce(sum(usd_cover_amount) filter (where is_within_period), 0) as usd_cover_7day,
  coalesce(sum(usd_premium_amount) filter (where is_within_period), 0) as usd_premium_7day,
  coalesce(sum(usd_cover_amount) filter (where cover_id in (select cover_id from new_cover_buyers)), 0) as usd_cover_7day_1st_buyers,
  coalesce(sum(usd_premium_amount) filter (where cover_id in (select cover_id from new_cover_buyers)), 0) as usd_premium_7day_1st_buyers
from covers
