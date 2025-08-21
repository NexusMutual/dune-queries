with

covers_v2 as (
  select
    cover_id,
    product_type,
    product_name,
    cover_start_usd,
    cover_start_eth,
    sum(premium_nxm) as premium_nxm,
    sum(premium_usd) as premium_usd
  from query_3810247 -- full list of covers v2
  where 1 = 1
    --and cover_start_time >= timestamp '2024-08-01'
    and product_type in (
      'DeFi Pass',
      'Follow On Cover',
      'Nexus Mutual Cover',
      'Crypto Cover',
      'Native Syndicate Cover',
      'Generalised Fund Portfolio Cover',
      'Fund Portfolio Cover'
    )
  group by 1, 2, 3, 4, 5
)

select
  product_type,
  count(distinct cover_id) as cover_count,
  sum(cover_start_usd) as total_cover_usd,
  sum(cover_start_eth) as total_cover_eth,
  avg(cover_start_usd) as avg_cover_usd,
  avg(cover_start_eth) as avg_cover_eth,
  sum(premium_nxm) as total_premium_nxm,
  sum(premium_usd) as total_premium_usd,
  avg(premium_nxm) as avg_premium_nxm,
  avg(premium_usd) as avg_premium_usd
from covers_v2
group by 1
order by 1
