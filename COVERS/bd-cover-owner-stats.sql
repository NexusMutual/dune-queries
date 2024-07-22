with

cover_sales_per_owner as (
  select *
  from query_3913267 -- BD cover owners base
  where last_cover_buy >= now() - interval '12' month
)

select
  1.0000 * sum(cover_sold) / count(distinct cover_owner) as mean_cover_wallet,
  sum(eth_cover) / count(distinct cover_owner) as mean_cover_amount_wallet,
  sum(eth_premium) / count(distinct cover_owner) as mean_premium_wallet,
  1.0000 * sum(product_sold) / count(distinct cover_owner) as mean_products_wallet
from cover_sales_per_owner
