with

params as (
  select cast(case '{{month}}'
      when 'current month' then cast(date_trunc('month', current_date) as varchar)
      when 'last month' then cast(date_add('month', -1, date_trunc('month', current_date)) as varchar)
      else '{{month}}'
    end as timestamp) as period_date
),

covers_v2 as (
  select
    cover_id,
    product_name,
    cover_start_usd,
    cover_start_eth,
    sum(premium_nxm) as premium_nxm,
    sum(premium_usd) as premium_usd
  from query_3810247 -- full list of covers v2
  where date_trunc('month', cover_start_time) in (select period_date from params)
    and cover_start_usd >= 1000000
  group by 1, 2, 3, 4
)

select
  product_name,
  sum(cover_start_usd) as cover_usd,
  sum(cover_start_eth) as cover_eth,
  sum(premium_nxm) as premium_nxm,
  sum(premium_usd) as premium_usd
from covers_v2
group by 1
order by 1, 2 desc
