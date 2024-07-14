with

cover_sales_per_owner as (
  select
    coalesce(ens.name, cast(co.cover_owner as varchar)) as cover_owner_ens,
    *
  from query_3913267 co -- BD cover owners base
    left join labels.ens on co.cover_owner = ens.address
)
select
  cover_owner_ens as cover_owner,
  cover_sold,
  product_sold,
  mean_product_sold
from cover_sales_per_owner
order by mean_product_sold desc
limit 10
