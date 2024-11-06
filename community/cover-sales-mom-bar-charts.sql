select
  date_trunc('month', block_date) as period_date,
  sum(eth_cover) as eth_cover,
  sum(usd_cover) as usd_cover,
  sum(eth_premium) as eth_premium,
  sum(usd_premium) as usd_premium
--from query_3889661 -- BD active cover base
from nexusmutual_ethereum.covers_daily_agg
where block_date >= date_add('month', -24, date_trunc('month', current_date))
group by 1
order by 1
