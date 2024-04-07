/*
select
  poolId as pool_id,
  stakingPoolAddress as pool_address
from nexusmutual_ethereum.StakingPoolFactory_evt_StakingPoolCreated
order by 1

select * from nexusmutual_ethereum.TokenController_call_assignStakingPoolManager
*/

with

staking_pools as (
  select
    call_block_time as block_time,
    output_0 as pool_id,
    output_1 as pool_address,
    isPrivatePool as is_private_pool,
    initialPoolFee as initial_pool_fee,
    maxPoolFee as max_management_fee,
    productInitParams as params
  from nexusmutual_ethereum.Cover_call_createStakingPool
  where call_success
    and contract_address = 0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62
),

staking_pools_and_products as (
  select
    sp.block_time,
    sp.pool_id,
    sp.pool_address,
    sp.is_private_pool,
    sp.initial_pool_fee,
    sp.max_management_fee,
    cast(json_query(t.json, 'lax $.productId') as int) as product_id,
    cast(json_query(t.json, 'lax $.weight') as int) as weight,
    cast(json_query(t.json, 'lax $.initialPrice') as int) as initial_price,
    cast(json_query(t.json, 'lax $.targetPrice') as int) as target_price
  from staking_pools as sp
    cross join unnest(params) as t(json)
),

staking_pool_updates as (
  select
    *,
    row_number() over (partition by pool_id, product_id order by block_time desc) as rn
  from (
    select
      call_block_time as block_time,
      poolId as pool_id,
      cast(json_query(t.json, 'lax $.productId') as int) as product_id,
      cast(json_query(t.json, 'lax $.recalculateEffectiveWeight') as boolean) as re_eval_eff_weight,
      cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) as set_target_weight,
      cast(json_query(t.json, 'lax $.targetWeight') as double) as target_weight,
      cast(json_query(t.json, 'lax $.setTargetPrice') as boolean) as set_target_price,
      cast(json_query(t.json, 'lax $.targetPrice') as double) as target_price
    from nexusmutual_ethereum.StakingProducts_call_setProducts as p
      cross join unnest(params) as t(json)
    where call_success
      and contract_address = 0xcafea573fBd815B5f59e8049E71E554bde3477E4
      and cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) = true
  ) t
),

staking_pool_products_combined as (
  select
    coalesce(spp.pool_id, spu.pool_id) as pool_id,
    coalesce(spp.product_id, spu.product_id) as product_id,
    spp.initial_price,
    spp.target_price,
    spu.target_price as updated_target_price,
    spp.weight as initial_weight,
    spu.target_weight as updated_target_weight,
    spu.block_time as updated_time
  from staking_pools_and_products spp
    full outer join staking_pool_updates as spu on spp.pool_id = spu.pool_id and spp.product_id = spu.product_id
  where coalesce(spu.rn, 1) = 1
)

select
  sp.pool_id,
  sp.pool_address,
  sp.is_private_pool,
  sp.initial_pool_fee,
  sp.max_management_fee,
  spc.product_id,
  spc.initial_price,
  spc.target_price,
  spc.updated_target_price,
  spc.initial_weight,
  spc.updated_target_weight,
  sp.block_time as created_time,
  spc.updated_time
from staking_pools as sp
  inner join staking_pool_products_combined as spc on sp.pool_id = spc.pool_id
order by pool_id, product_id
