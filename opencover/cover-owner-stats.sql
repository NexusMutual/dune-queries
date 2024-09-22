with

cover_sales_per_owner as (
  select *
  from query_4086950 -- cover owner overview
  where last_cover_buy >= now() - interval '12' month
)

select
  sum(cast(cover_sold as double)) / count(distinct cover_owner) as mean_cover_wallet,
  sum(usd_cover_amount) / count(distinct cover_owner) as mean_cover_amount_wallet,
  sum(usd_premium_amount) / count(distinct cover_owner) as mean_premium_wallet,
  sum(cast(product_sold as double)) / count(distinct cover_owner) as mean_products_wallet,
  approx_percentile(cast(cover_sold as double), 0.5) as median_cover_wallet,
  approx_percentile(usd_cover_amount, 0.5) as median_cover_amount_wallet,
  approx_percentile(usd_premium_amount, 0.5) as median_premium_wallet,
  approx_percentile(cast(product_sold as double), 0.5) as median_products_wallet
from cover_sales_per_owner
