with

avg_prices as (
  select
    date_trunc('month', block_date) as month_date,
    min(avg_eth_usd_price) as avg_eth_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
  group by 1
),

monthly_aggs as (
  select
    date_trunc('month', block_date) as month_date,
    sum(cover_sold) as cover_sold,
    sum(eth_cover) as eth_cover,
    sum(eth_eth_cover) as eth_eth_cover,
    sum(dai_eth_cover) as dai_eth_cover,
    sum(usdc_eth_cover) as usdc_eth_cover,
    sum(cbbtc_eth_cover) as cbbtc_eth_cover,
    sum(usd_cover) as usd_cover,
    sum(eth_usd_cover) as eth_usd_cover,
    sum(dai_usd_cover) as dai_usd_cover,
    sum(usdc_usd_cover) as usdc_usd_cover,
    sum(cbbtc_usd_cover) as cbbtc_usd_cover
  from query_5778799 -- bd cover - base root
  where block_date >= date_add('month', -24, date_trunc('month', current_date))
  group by 1
)

select
  a.month_date,
  p.avg_eth_usd_price,
  a.cover_sold,
  a.eth_cover,
  a.eth_eth_cover,
  a.dai_eth_cover,
  a.usdc_eth_cover,
  a.cbbtc_eth_cover,
  a.usd_cover,
  a.eth_usd_cover,
  a.dai_usd_cover,
  a.usdc_usd_cover,
  a.cbbtc_usd_cover
from monthly_aggs a
  inner join avg_prices p on a.month_date = p.month_date
order by 1 desc
