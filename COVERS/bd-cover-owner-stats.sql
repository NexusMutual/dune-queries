with

cover_sales_per_owner as (
  select *
  from query_3913267 -- BD cover owners base
  where last_cover_buy >= now() - interval '12' month
    and cover_owner <> 0x181aea6936b407514ebfc0754a37704eb8d98f91
)

select
  sum(cast(cover_sold as double)) / count(distinct cover_owner) as mean_cover_wallet,
  sum(eth_cover) / count(distinct cover_owner) as mean_cover_amount_wallet,
  sum(eth_premium) / count(distinct cover_owner) as mean_premium_wallet,
  sum(cast(product_sold as double)) / count(distinct cover_owner) as mean_products_wallet,
  approx_percentile(cast(cover_sold as double), 0.5) as median_cover_wallet,
  approx_percentile(eth_cover, 0.5) as median_cover_amount_wallet,
  approx_percentile(eth_premium, 0.5) as median_premium_wallet,
  approx_percentile(cast(product_sold as double), 0.5) as median_products_wallet
from cover_sales_per_owner
