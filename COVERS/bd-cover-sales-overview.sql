select
  block_date,
  -- cover sales
  eth_cover,
  usd_cover,
  cover_sold,
  coalesce(eth_cover / nullif(cover_sold, 0), 0) as mean_eth_cover,
  median_eth_cover,
  coalesce(usd_cover / nullif(cover_sold, 0), 0) as mean_usd_cover,
  median_usd_cover,
  eth_premium,
  usd_premium,
  coalesce(eth_premium / nullif(cover_sold, 0), 0) as mean_eth_premium,
  median_eth_premium,
  coalesce(usd_premium / nullif(cover_sold, 0), 0) as mean_usd_premium,
  median_usd_premium
from query_3889661 -- BD active cover base
order by 1 desc
