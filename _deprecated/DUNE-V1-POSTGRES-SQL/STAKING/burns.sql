WITH
  v1_product_info as (
    select
      "contract_address" as product_address,
      "syndicate" as syndicate,
      "name" as product_name,
      "type" as product_type
    from
      dune_user_generated.nexus_v1_product_info_view
  ),
  burns as (
    select
      date_trunc('day', "evt_block_time") as day,
      "amount" * 1E-18 as sum_burnt,
      syndicate,
      product_name,
      product_type,
      product_address
    from
      nexusmutual."PooledStaking_evt_Burned"
      INNER JOIN v1_product_info ON v1_product_info.product_address = nexusmutual."PooledStaking_evt_Burned"."contractAddress"
  )
select
  distinct day,
  SUM(sum_burnt) OVER (PARTITION BY product_address) as total_burnt_per_product,
  SUM(sum_burnt) OVER (
    ORDER by
      day
  ) as total_burnt,
  syndicate,
  product_name,
  product_type,
  product_address
from
  burns