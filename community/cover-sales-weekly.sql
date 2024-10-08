select
  product_name,
  sum(cover_start_usd) as cover_usd,
  sum(cover_start_eth) as cover_eth,
  sum(premium_nxm) as premium_nxm,
  sum(premium_usd) as premium_usd
from query_3810247 -- full list of covers v2
where date_trunc('week', cover_start_time) = cast(case '{{week}}'
    when 'current week' then cast(date_trunc('week', current_date) as varchar)
    when 'last week' then cast(date_add('week', -1, date_trunc('week', current_date)) as varchar)
    else '{{week}}'
  end as timestamp)
group by 1
order by 1
