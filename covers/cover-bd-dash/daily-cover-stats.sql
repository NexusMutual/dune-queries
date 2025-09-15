with daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
)

select
  c.block_date,
  p.avg_eth_usd_price,
  active_cover,
  eth_active_cover,
  eth_eth_active_cover,
  dai_eth_active_cover,
  usdc_eth_active_cover,
  cbbtc_eth_active_cover,
  eth_active_cover / active_cover as mean_eth_active_cover,
  median_eth_active_cover,
  usd_active_cover,
  eth_usd_active_cover,
  dai_usd_active_cover,
  usdc_usd_active_cover,
  cbbtc_usd_active_cover,
  usd_active_cover / active_cover as mean_usd_active_cover,
  median_usd_active_cover,
  eth_active_premium,
  eth_eth_active_premium,
  dai_eth_active_premium,
  usdc_eth_active_premium,
  cbbtc_eth_active_premium,
  nxm_eth_active_premium,
  eth_active_premium / active_cover as mean_eth_active_premium,
  median_eth_active_premium,
  usd_active_premium,
  eth_usd_active_premium,
  dai_usd_active_premium,
  usdc_usd_active_premium,
  cbbtc_usd_active_premium,
  nxm_usd_active_premium,
  usd_active_premium / active_cover as mean_usd_active_premium,
  median_usd_active_premium
from query_5778799 c -- bd cover - base root
  inner join daily_avg_prices p on c.block_date = p.block_date
where c.block_date >= now() - interval '6' month
order by 1 desc
