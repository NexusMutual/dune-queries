select
  block_date,
  -- active cover
  eth_active_cover,
  usd_active_cover,
  active_cover_sold,
  eth_active_cover / active_cover_sold as mean_eth_active_cover,
  usd_active_cover / active_cover_sold as mean_usd_active_cover,
  eth_active_premium,
  usd_active_premium,
  eth_active_premium / active_cover_sold as mean_eth_active_premium,
  usd_active_premium / active_cover_sold as mean_usd_active_premium
from query_3889661 -- BD active cover base
order by 1 desc
