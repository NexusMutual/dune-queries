select
  date_trunc('quarter', cover_start_time) as period_date,
  product_type,
  sum(cover_start_usd) as cover_usd,
  sum(cover_start_eth) as cover_eth,
  sum(premium_nxm) as premium_nxm,
  sum(premium_usd) as premium_usd
from query_3810247 -- full list of covers v2
where date_trunc('quarter', cover_start_time) >= date_add('quarter', -12, date_trunc('quarter', current_date))
group by 1, 2
order by 1, 2
