select
  date_trunc('quarter', block_date) as period_date,
  --sum(cover_sold) as cover_sold,
  sum(usd_cover) as usd_cover,
  sum(eth_cover) as eth_cover,
  sum(usd_premium) as usd_premium,
  sum(eth_premium) as eth_premium
--from query_3889661 -- BD active cover base
from nexusmutual_ethereum.covers_daily_agg
group by 1
order by 1 desc
