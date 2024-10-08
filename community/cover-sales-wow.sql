select
  date_trunc('week', block_date) as period_date,
  sum(cover_sold) as cover_sold,
  sum(eth_cover) as eth_cover,
  sum(usd_cover) as usd_cover,
  sum(eth_premium) as eth_premium,
  sum(usd_premium) as usd_premium
--from query_3889661 -- BD active cover base
from nexusmutual_ethereum.covers_daily_agg
where block_date >= date_add('year', -2, current_date)
group by 1
order by 1 desc
