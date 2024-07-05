select
  block_date,
  -- cover sales
  eth_cover,
  usd_cover,
  cover_sold,
  eth_cover / nullif(cover_sold, 0) as mean_eth_cover,
  usd_cover / cover_sold as mean_usd_cover,
  eth_premium,
  usd_premium,
  eth_premium / cover_sold as mean_eth_premium,
  usd_premium / cover_sold as mean_usd_premium
from query_3889661 -- BD active cover base
order by 1 desc
