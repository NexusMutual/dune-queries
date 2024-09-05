with

staking_pools_evt as (
  select
    evt_block_time as block_time_created,
    poolId as pool_id,
    stakingPoolAddress as pool_address,
    evt_tx_hash as tx_hash_created
  from nexusmutual_ethereum.StakingPoolFactory_evt_StakingPoolCreated
),

staking_pools_created as (
  select
    call_block_time as block_time_created,
    output_0 as pool_id,
    output_1 as pool_address,
    isPrivatePool as is_private_pool,
    initialPoolFee as initial_pool_fee,
    maxPoolFee as max_management_fee,
    productInitParams as params,
    call_tx_hash as tx_hash_created
  from nexusmutual_ethereum.Cover_call_createStakingPool
  where call_success
    and contract_address = 0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62
  union all
    select
    call_block_time as block_time_created,
    output_0 as pool_id,
    output_1 as pool_address,
    isPrivatePool as is_private_pool,
    initialPoolFee as initial_pool_fee,
    maxPoolFee as max_management_fee,
    productInitParams as params,
    call_tx_hash as tx_hash_created
  from nexusmutual_ethereum.StakingProducts_call_createStakingPool
  where call_success
),

staking_pools_created_ext as (
  select
    spe.block_time_created,
    spe.pool_id,
    spe.pool_address,
    spc.is_private_pool,
    spc.initial_pool_fee,
    spc.max_management_fee,
    spc.params,
    spe.tx_hash_created
  from staking_pools_evt spe
    inner join staking_pools_created spc on spe.pool_id = spc.pool_id and spe.block_time_created = spc.block_time_created
),

staking_pools_and_products as (
  select
    sp.block_time_created,
    sp.pool_id,
    sp.pool_address,
    sp.is_private_pool,
    sp.initial_pool_fee,
    sp.max_management_fee,
    cast(json_query(t.json, 'lax $.productId') as int) as product_id,
    cast(json_query(t.json, 'lax $.weight') as double) as weight,
    cast(json_query(t.json, 'lax $.initialPrice') as double) as initial_price,
    cast(json_query(t.json, 'lax $.targetPrice') as double) as target_price,
    sp.tx_hash_created
  from staking_pools_created_ext as sp
    left join unnest(sp.params) as t(json) on true
),

staking_pool_products_updated as (
  select
    *,
    row_number() over (partition by pool_id, product_id order by block_time_updated desc) as rn
  from (
    select
      p.call_block_time as block_time_updated,
      p.poolId as pool_id,
      cast(json_query(t.json, 'lax $.productId') as int) as product_id,
      cast(json_query(t.json, 'lax $.recalculateEffectiveWeight') as boolean) as re_eval_eff_weight,
      cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) as set_target_weight,
      cast(json_query(t.json, 'lax $.targetWeight') as double) as target_weight,
      cast(json_query(t.json, 'lax $.setTargetPrice') as boolean) as set_target_price,
      cast(json_query(t.json, 'lax $.targetPrice') as double) as target_price,
      p.call_tx_hash as tx_hash_updated
    from nexusmutual_ethereum.StakingProducts_call_setProducts as p
      cross join unnest(params) as t(json)
    where p.call_success
      and p.contract_address = 0xcafea573fBd815B5f59e8049E71E554bde3477E4
      and cast(json_query(t.json, 'lax $.setTargetWeight') as boolean) = true
  ) t
),

staking_pool_products_combined as (
  select
    spp.block_time_created,
    spu.block_time_updated,
    coalesce(spp.pool_id, spu.pool_id) as pool_id,
    coalesce(spp.product_id, spu.product_id) as product_id,
    spp.initial_price,
    spp.target_price,
    spu.target_price as updated_target_price,
    spp.weight as initial_weight,
    spu.target_weight,
    if(spp.product_id is null, true, false) as is_product_added,
    spp.tx_hash_created,
    spu.tx_hash_updated
  from staking_pools_and_products spp
    full outer join staking_pool_products_updated as spu on spp.pool_id = spu.pool_id and spp.product_id = spu.product_id
  where coalesce(spu.rn, 1) = 1
),

staking_pool_managers_history as (
  select
    call_block_time as block_time,
    poolId as pool_id,
    manager,
    call_trace_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.TokenController_call_assignStakingPoolManager
  where call_success
  union all
  select
    call_block_time as block_time,
    poolId as pool_id,
    manager,
    call_trace_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.TokenController2_call_assignStakingPoolManager
  where call_success
  union all
  select
    call_block_time as block_time,
    poolId as pool_id,
    manager,
    call_trace_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.TokenController3_call_assignStakingPoolManager
  where call_success
  union all
  select distinct
    m.call_block_time as block_time,
    sp.poolId as pool_id,
    m.output_0 as manager,
    m.call_trace_address,
    m.call_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingProducts_evt_ProductUpdated pu
    inner join nexusmutual_ethereum.StakingProducts_call_setProducts sp on pu.evt_tx_hash = call_tx_hash and pu.evt_block_number = sp.call_block_number
    inner join nexusmutual_ethereum.StakingPool_call_manager m on sp.call_tx_hash = m.call_tx_hash and sp.call_block_number = m.call_block_number
  where sp.call_success
    and m.call_success
),

staking_pool_managers as (
  select
    t.block_time as block_time_updated,
    t.pool_id,
    t.manager as manager_address,
    ens.name as manager_ens,
    coalesce(ens.name, cast(t.manager as varchar)) as manager,
    t.tx_hash as tx_hash_updated
  from (
      select
        block_time,
        pool_id,
        manager,
        tx_hash,
        row_number() over (partition by pool_id order by block_time, call_trace_address desc) as rn
      from staking_pool_managers_history
    ) t
    left join labels.ens on t.manager = ens.address
  where t.rn = 1
),

staking_pool_fee_updates as (
  select
    block_time as block_time_updated,
    pool_address,
    new_fee,
    tx_hash as tx_hash_updated
  from (
      select
        evt_block_time as block_time,
        contract_address as pool_address,
        newFee as new_fee,
        evt_tx_hash as tx_hash,
        row_number() over (partition by contract_address order by evt_block_time desc, evt_index desc) as rn
      from nexusmutual_ethereum.StakingPool_evt_PoolFeeChanged
    ) t
  where t.rn = 1
),

products as (
  select
    p.product_id,
    p.product_name,
    pt.product_type_id,
    pt.product_type_name as product_type
  --from query_3788361 pt -- product types
  --  inner join query_3788363 p -- products
  from nexusmutual_ethereum.product_types_v2 pt
    inner join nexusmutual_ethereum.products_v2 p
      on pt.product_type_id = p.product_type_id
)

select
  sp.block_time_created,
  spc.block_time_updated as block_time_product_updated,
  spm.block_time_updated as block_time_manager_updated,
  spf.block_time_updated as block_time_fee_updated,
  greatest(
    coalesce(spc.block_time_updated, sp.block_time_created),
    coalesce(spm.block_time_updated, sp.block_time_created),
    coalesce(spf.block_time_updated, sp.block_time_created)
  ) as block_time_updated,
  sp.pool_id,
  sp.pool_address,
  spm.manager_address,
  spm.manager_ens,
  spm.manager,
  sp.is_private_pool,
  sp.initial_pool_fee / 100.00 as initial_pool_fee,
  coalesce(spf.new_fee, sp.initial_pool_fee) / 100.00 as current_management_fee,
  sp.max_management_fee / 100.00 as max_management_fee,
  spc.product_id,
  spc.initial_price / 100.00 as initial_price,
  coalesce(spc.updated_target_price, spc.target_price) / 100.00 as target_price,
  spc.initial_weight / 100.00 as initial_weight,
  spc.target_weight / 100.00 as target_weight,
  sp.block_time_created as pool_created_time,
  if(spc.is_product_added, spc.block_time_updated, sp.block_time_created) as product_added_time,
  sp.tx_hash_created,
  spc.tx_hash_updated as tx_hash_product_updated,
  spm.tx_hash_updated as tx_hash_manager_updated,
  spf.tx_hash_updated as tx_hash_fee_updated,
  greatest(
    coalesce(spc.tx_hash_updated, sp.tx_hash_created),
    coalesce(spm.tx_hash_updated, sp.tx_hash_created),
    coalesce(spf.tx_hash_updated, sp.tx_hash_created)
  ) as tx_hash_updated
from staking_pools_created_ext sp
  inner join staking_pool_products_combined spc on sp.pool_id = spc.pool_id
  left join staking_pool_managers spm on sp.pool_id = spm.pool_id
  left join staking_pool_fee_updates spf on sp.pool_address = spf.pool_address
