with

btc_cover_buyers as (
  select distinct
    cover_id,
    cover_owner,
    native_cover_amount as cover_amount
  from query_3810247 -- NM covers v2 full list
  where cover_asset = 'cbBTC'
    and native_cover_amount >= 0.01
    and cover_owner not in (0x40329f3e27dd3fe228799b4a665f6f104c2ab6b4)
  union all
  select
    cover_id,
    cover_owner,
    cover_amount
  from query_3957542 -- OC covers
  where cover_asset = 'cbBTC'
    and cover_amount >= 0.01
),

btc_cover_buyers_agg as (
  select
    cover_owner,
    count(cover_id) as cover_buys,
    sum(cover_amount) as total_cover_amount
  from btc_cover_buyers
  group by 1
)

select
  cover_owner,
  cover_buys,
  total_cover_amount,
  total_cover_amount / (select total_cover_amount from btc_cover_buyers_agg order by 1 desc limit 1) as total_cover_amount_ratio
from btc_cover_buyers_agg
order by 3 desc
