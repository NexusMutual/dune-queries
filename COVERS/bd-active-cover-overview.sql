select
  block_date,
  -- active cover
  active_cover,
  eth_active_cover,
  usd_active_cover,
  eth_active_cover / active_cover as mean_eth_active_cover,
  median_eth_active_cover,
  usd_active_cover / active_cover as mean_usd_active_cover,
  median_usd_active_cover,
  eth_active_premium,
  usd_active_premium,
  eth_active_premium / active_cover as mean_eth_active_premium,
  median_eth_active_premium,
  usd_active_premium / active_cover as mean_usd_active_premium,
  median_usd_active_premium
--from query_3889661 -- BD active cover base
from nexusmutual_ethereum.covers_daily_agg
order by 1 desc
