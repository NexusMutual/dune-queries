select
  product_type,
  sum(cover_start_usd) as cover_usd,
  sum(cover_start_eth) as cover_eth,
  sum(premium_nxm) as premium_nxm,
  sum(premium_usd) as premium_usd
from query_3810247 -- full list of covers v2
where date_trunc('quarter', cover_start_time) = cast(case '{{quarter}}'
    when 'current quarter' then cast(date_trunc('quarter', current_date) as varchar)
    when 'last quarter' then cast(date_add('quarter', -1, date_trunc('quarter', current_date)) as varchar)
    else '{{quarter}}'
  end as timestamp)
group by 1
order by 1
