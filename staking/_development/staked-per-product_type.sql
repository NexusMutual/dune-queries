with

staked_per_product_data as (
  select
    pool_id,
    pool_name,
    product_id,
    product_name,
    product_type,
    target_weight,
    nxm_allocated,
    nxm_max_capacity,
    nxm_usd_max_capacity,
    nxm_eth_max_capacity,
    active_cover_count,
    usd_cover_amount,
    eth_cover_amount,
    usd_available_capacity,
    eth_available_capacity
  from query_3991071 -- staking pools products overview (staked per product)
)

select
  pool_id,
  pool_name,
  product_type,
  sum(nxm_allocated) as nxm_allocated,
  sum(nxm_max_capacity) as nxm_max_capacity,
  sum(nxm_usd_max_capacity) as nxm_usd_max_capacity,
  sum(nxm_eth_max_capacity) as nxm_eth_max_capacity,
  sum(active_cover_count) as active_cover_count,
  sum(usd_cover_amount) as usd_cover_amount,
  sum(eth_cover_amount) as eth_cover_amount,
  sum(usd_available_capacity) as usd_available_capacity,
  sum(eth_available_capacity) as eth_available_capacity
from staked_per_product_data
group by 1,2,3
order by 1,3
