with

staking_pools_base as (
  select *, row_number() over (partition by pool_id, product_id order by block_time_updated desc) as rn
  from nexusmutual_ethereum.staking_pools
)

select
  block_time_created,
  block_time_updated,
  pool_id,
  pool_address,
  manager_address,
  manager_ens,
  manager,
  is_private_pool,
  initial_pool_fee,
  current_management_fee,
  max_management_fee,
  product_id,
  product_name,
  product_type,
  initial_price,
  target_price,
  initial_weight,
  target_weight,
  pool_created_time,
  product_added_time,
  tx_hash_created,
  tx_hash_updated
from staking_pools_base
where rn = 1
