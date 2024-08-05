select
  block_date,
  active_cover,
  eth_active_cover,
  eth_eth_active_cover,
  dai_eth_active_cover,
  usdc_eth_active_cover,
  eth_active_cover / active_cover as mean_eth_active_cover,
  median_eth_active_cover,
  usd_active_cover,
  eth_usd_active_cover,
  dai_usd_active_cover,
  usdc_usd_active_cover,
  usd_active_cover / active_cover as mean_usd_active_cover,
  median_usd_active_cover,
  eth_active_premium,
  eth_eth_active_premium,
  dai_eth_active_premium,
  usdc_eth_active_premium,
  nxm_eth_active_premium,
  eth_active_premium / active_cover as mean_eth_active_premium,
  median_eth_active_premium,
  usd_active_premium,
  eth_usd_active_premium,
  dai_usd_active_premium,
  usdc_usd_active_premium,
  nxm_usd_active_premium,
  usd_active_premium / active_cover as mean_usd_active_premium,
  median_usd_active_premium
--from query_3889661 -- BD active cover base
from nexusmutual_ethereum.covers_daily_agg
where block_date >= now() - interval '6' month
order by 1 desc
