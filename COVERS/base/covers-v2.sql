with

cover_sales as (
  select
    c.call_block_time as block_time,
    c.call_block_number as block_number,
    c.output_coverId as cover_id,
    c.call_block_time as cover_start_time,
    date_add('second', cast(json_query(c.params, 'lax $.period') as bigint), c.call_block_time) as cover_end_time,
    cast(json_query(c.params, 'lax $.period') as bigint) as cover_period_seconds,
    cast(json_query(t.pool_allocation, 'lax $.poolId') as uint256) as pool_id,
    cast(json_query(c.params, 'lax $.productId') as uint256) as product_id,
    from_hex(json_query(c.params, 'lax $.owner' omit quotes)) as cover_owner,
    cast(json_query(c.params, 'lax $.amount') as uint256) as sum_assured,
    cast(json_query(c.params, 'lax $.coverAsset') as int) as cover_asset,
    cast(json_query(c.params, 'lax $.paymentAsset') as int) as payment_asset,
    cast(json_query(c.params, 'lax $.maxPremiumInAsset') as uint256) as max_premium_in_asset,
    cast(json_query(c.params, 'lax $.commissionRatio') as double) as commission_ratio,
    from_hex(json_query(c.params, 'lax $.commissionDestination' omit quotes)) as commission_destination,
    cast(json_query(t.pool_allocation, 'lax $.coverAmountInAsset') as uint256) as cover_amount_in_asset,
    cast(json_query(t.pool_allocation, 'lax $.skip') as boolean) as pool_allocation_skip,
    c.call_trace_address,
    c.call_tx_hash as tx_hash
  from nexusmutual_ethereum.Cover_call_buyCover c
    cross join unnest(c.poolAllocationRequests) as t(pool_allocation)
  where c.call_success
),

staking_product_premiums as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    poolId as pool_id,
    productId as product_id,
    --call_block_time as premium_start_time, -- same as cover_start_time
    --date_add('second', cast(period as bigint), call_block_time) as premium_end_time, -- same as cover_end_time
    output_premium as premium,
    coverAmount as cover_amount,
    initialCapacityUsed as initial_capacity_used,
    totalCapacity as total_capacity,
    globalMinPrice as global_min_price,
    useFixedPrice as use_fixed_price,
    nxmPerAllocationUnit as nxm_per_allocation_unit,
    allocationUnitsPerNXM as allocation_units_per_nxm,
    cast(period as bigint) as premium_period_seconds,
    case
      when date_add('second', cast(period as bigint), call_block_time) > now()
      then (to_unixTime(now()) - to_unixTime(call_block_time)) / cast(period as bigint)
      else 1
    end as premium_period_ratio,
    call_trace_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingProducts_call_getPremium
  where call_success
    and contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4
),

cover_premiums as (
  select
    c.block_time,
    c.block_number,
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    c.pool_id,
    c.product_id,
    p.cover_amount / 100.0 as partial_cover_amount, -- partial_cover_amount_in_nxm
    p.premium / 1e18 as premium,
    c.commission_ratio / 10000.0 as commission_ratio,
    --(1.0 + (c.commission_ratio / 10000.0)) * (0.5 / (1.0 + (c.commission_ratio / 10000.0))) * p.premium / 1e18 as premium,
    --(1.0 + (c.commission_ratio / 10000.0)) * p.premium / 1e18 as premium,
    case c.cover_asset
      when 0 then 'ETH'
      when 1 then 'DAI'
      else 'NA'
    end as cover_asset,
    c.sum_assured / 1e18 as sum_assured,
    case c.payment_asset
      when 0 then 'ETH'
      when 1 then 'DAI'
      when 255 then 'NXM'
      else 'NA'
    end as premium_asset,
    c.cover_owner,
    c.commission_destination,
    c.tx_hash

    /*
    -- combine below with staking pools overview?
    p.premium / 1e18 as premium,
    p.premium / 1e18 * 0.5 as commission,
    (c.commission_ratio / 10000.0) * p.premium / 1e18 * 0.5 as commission_distibutor_fee,
    p.premium / 1e18 * 0.5 * h.pool_fee as pool_manager_commission,
    p.premium / 1e18 * 0.5 * (1 - h.pool_fee) as staker_commission,
    p.premium_period_ratio * p.premium / 1e18 * 0.5 as commission_emitted,
    p.premium_period_ratio * p.premium / 1e18 * 0.5 * h.pool_fee as pool_manager_commission_emitted,
    p.premium_period_ratio * p.premium / 1e18 * 0.5 * (1 - h.pool_fee) as staker_commission_emitted
    */

  from cover_sales c
    inner join staking_product_premiums p on c.tx_hash = p.tx_hash and c.block_number = p.block_number
      and c.pool_id = p.pool_id and c.product_id = p.product_id
),

products as (
  select
    p.product_id,
    p.product_name,
    pt.product_type_id,
    pt.product_type_name as product_type
  from query_3676071 pt -- product types
    inner join query_3676060 p -- products
      on pt.product_type_id = p.product_type_id
),

covers_v2 as (
  select
    cp.block_time,
    cp.block_number,
    cp.cover_id,
    cp.cover_start_time,
    cp.cover_end_time,
    cp.pool_id,
    cp.product_id,
    p.product_type,
    p.product_name,
    cp.partial_cover_amount, -- partial_cover_amount_in_nxm
    --cp.pool_premium,
    cp.commission_ratio,
    cp.cover_asset,
    cp.sum_assured,
    cp.premium_asset,
    cp.premium,
    cp.cover_owner,
    cp.commission_destination,
    cp.tx_hash
  from cover_premiums cp
    left join products p on cp.product_id = p.product_id
),

covers_v1_migrated as (
  select
    cm.evt_block_time as block_time,
    cm.evt_block_number as block_number,
    cm.coverIdV2 as cover_id,
    cv1.cover_start_time,
    cv1.cover_end_time,
    cv1.premium,
    cv1.premium_nxm,
    cv1.sum_assured,
    cv1.syndicate,
    cv1.product_name,
    cv1.product_type,
    cv1.cover_asset,
    cv1.premium_asset,
    cm.newOwner as cover_owner,
    cm.evt_index,
    cm.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.CoverMigrator_evt_CoverMigrated cm
    inner join query_3678366 cv1 -- covers v1
      on cm.coverIdV1 = cv1.cover_id
),

covers as (
  select
    block_time,
    block_number,
    cover_id,
    cover_start_time,
    cover_end_time,
    cast(pool_id as varchar) as staking_pool,
    product_type,
    product_name,
    cover_asset,
    premium_asset,
    premium,
    premium as premium_nxm,
    sum_assured,
    partial_cover_amount, -- in NMX
    cover_owner,
    false as is_migrated,
    tx_hash
  from covers_v2
  union all
  select
    block_time,
    block_number,
    cover_id,
    cover_start_time,
    cover_end_time,
    syndicate as staking_pool,
    product_type,
    product_name,
    cover_asset,
    premium_asset,
    premium,
    premium_nxm,
    sum_assured,
    sum_assured as partial_cover_amount, -- No partial covers in v1 migrated covers
    cover_owner,
    true as is_migrated,
    tx_hash
  from covers_v1_migrated
)

select
  block_time,
  date_trunc('day', block_time) as block_date,
  block_number,
  cover_id,
  cover_start_time,
  cover_end_time,
  staking_pool,
  product_type,
  product_name,
  cover_asset,
  premium_asset,
  premium,
  premium_nxm,
  sum_assured,
  partial_cover_amount, -- in NMX
  cover_owner,
  is_migrated,
  tx_hash
from covers
--order by cp.cover_id desc
