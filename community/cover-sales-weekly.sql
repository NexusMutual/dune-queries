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
    sum(eth_active_cover) as eth_active_cover,
    sum(eth_eth_active_cover) as eth_eth_active_cover,
    sum(dai_eth_active_cover) as dai_eth_active_cover,
    sum(usdc_eth_active_cover) as usdc_eth_active_cover,
    sum(usd_active_cover) as usd_active_cover,
    sum(eth_usd_active_cover) as eth_usd_active_cover,
    sum(dai_usd_active_cover) as dai_usd_active_cover,
    sum(usdc_usd_active_cover) as usdc_usd_active_cover
  --from query_3889661 -- BD active cover base
  from nexusmutual_ethereum.covers_daily_agg
  where block_date >= date_add('week', -52, date_trunc('week', current_date))
  group by 1
)

select
  a.week_date,
  p.avg_eth_usd_price,
  a.eth_active_cover,
  a.eth_eth_active_cover,
  a.dai_eth_active_cover,
  a.usdc_eth_active_cover,
  a.usd_active_cover,
  a.eth_usd_active_cover,
  a.dai_usd_active_cover,
  a.usdc_usd_active_cover
from weekly_aggs a
  inner join avg_prices p on a.week_date = p.week_date
order by 1 desc
