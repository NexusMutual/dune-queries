with

migrated_covers as (
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
    cm.newOwner as owner,
    cm.evt_index,
    cm.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.CoverMigrator_evt_CoverMigrated cm
    inner join query_3678366 cv1 -- covers v1
      on cm.coverIdV1 = cv1.cover_id
),

cover_raw as (
  select
    c.call_block_time as block_time,
    c.call_block_number as block_number,
    c.output_coverId as cover_id,
    c.call_block_time as cover_start_time,
    date_add('second', cast(json_query(c.params, 'lax $.period') as bigint), c.call_block_time) as cover_end_time,
    cast(json_query(c.params, 'lax $.period') as bigint) as cover_period_seconds,
    cast(json_query(t.pool_allocation, 'lax $.poolId') as uint256) as pool_id,
    cast(json_query(c.params, 'lax $.productId') as uint256) as product_id,
    json_query(c.params, 'lax $.owner' omit quotes) as owner,
    cast(json_query(c.params, 'lax $.amount') as uint256) as sum_assured,
    cast(json_query(c.params, 'lax $.coverAsset') as int) as cover_asset,
    cast(json_query(c.params, 'lax $.paymentAsset') as int) as payment_asset,
    cast(json_query(c.params, 'lax $.maxPremiumInAsset') as uint256) as max_premium_in_asset,
    cast(json_query(c.params, 'lax $.commissionRatio') as int) as commission_ratio,
    json_query(c.params, 'lax $.commissionDestination' omit quotes) as commission_destination,
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
    call_block_time as premium_start_time,
    date_add('second', cast(period as bigint), call_block_time) as premium_end_time,
    output_premium as pool_premium,
    coverAmount / 100.0 as partial_cover_amount_in_nxm,
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

covers as (
  select
    c.cover_id,
    c.cover_start_time,
    c.pool_id,
    c.product_id,
    p.premium / 1e18 as premium,
    p.premium / 1e18 * 0.5 as commission,
    (c.commission_ratio / 10000.0) * p.premium / 1e18 * 0.5 as commission_distibutor_fee,
    p.premium / 1e18 * 0.5 * h.pool_fee as pool_manager_commission,
    p.premium / 1e18 * 0.5 * (1 - h.pool_fee) as staker_commission,
    p.premium_period_ratio * p.premium / 1e18 * 0.5 as commission_emitted,
    p.premium_period_ratio * p.premium / 1e18 * 0.5 * h.pool_fee as pool_manager_commission_emitted,
    p.premium_period_ratio * p.premium / 1e18 * 0.5 * (1 - h.pool_fee) as staker_commission_emitted
  from covers c
    inner join staking_product_premiums p on c.tx_hash = p.tx_hash and c.block_number = p.block_number
      and c.pool_id = p.pool_id and c.product_id = p.product_id
)

select *
from covers
