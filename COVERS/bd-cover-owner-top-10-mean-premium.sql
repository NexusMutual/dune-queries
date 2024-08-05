with

cover_sales_per_owner as (
  select
    coalesce(ens.name, cast(co.cover_owner as varchar)) as cover_owner_ens,
    *
  --from query_3913267 co -- BD cover owners base
  from nexusmutual_ethereum.cover_owners_agg co
    left join labels.ens on co.cover_owner = ens.address
)
select
  cover_owner_ens as cover_owner,
  eth_premium,
  mean_eth_premium,
  median_eth_premium,
  usd_premium,
  mean_usd_premium,
  median_usd_premium
from cover_sales_per_owner
order by mean_eth_premium desc
limit 10
