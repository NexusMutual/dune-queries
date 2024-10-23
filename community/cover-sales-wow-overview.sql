with

avg_prices as (
  select
    date_trunc('week', block_date) as week_date,
    min(avg_eth_usd_price) as avg_eth_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
  group by 1
),

weekly_aggs as (
  select
    date_trunc('week', block_date) as week_date,
    sum(usd_cover) as usd_cover,
    sum(eth_cover) as eth_cover,
    sum(usd_premium) as usd_premium,
    sum(eth_premium) as eth_premium
  --from query_3889661 -- BD active cover base
  from nexusmutual_ethereum.covers_daily_agg
  group by 1
)

select
  a.week_date,
  p.avg_eth_usd_price,
  a.usd_cover,
  a.eth_cover,
  a.usd_premium,
  a.eth_premium
from weekly_aggs a
  inner join avg_prices p on a.week_date = p.week_date
order by 1 desc
