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
  eth_cover,
  mean_eth_cover,
  median_eth_cover,
  usd_cover,
  mean_usd_cover,
  median_usd_cover
from cover_sales_per_owner
order by mean_eth_cover desc
limit 10
