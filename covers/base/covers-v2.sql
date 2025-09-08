with

cover_sales as (
  select
    c.call_block_time as block_time,
    c.call_block_number as block_number,
    c.output_coverId as cover_id,
    c.call_block_time as cover_start_time,
    -- retrieve params & poolAllocationRequests
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
    cast(json_query(c.params, 'lax $.ipfsData' omit quotes) as varchar) as cover_ipfs_data,
    c.call_trace_address as trace_address,
    c.call_tx_hash as tx_hash
  from nexusmutual_ethereum.Cover_call_buyCover c
    cross join unnest(c.poolAllocationRequests) as t(pool_allocation)
  where c.call_success
    and c.contract_address = 0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62 -- proxy
),

cover_renewals as (
  select
    c.call_block_time as block_time,
    c.call_block_number as block_number,
    c.output_coverId as cover_id,
    c.call_block_time as cover_start_time,
    -- retrieve params & poolAllocationRequests
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
    cast(json_query(c.params, 'lax $.ipfsData' omit quotes) as varchar) as cover_ipfs_data,
    -- retrieve executionDetails
    from_hex(json_query(c.executionDetails, 'lax $.buyer' omit quotes)) as renewal_buyer,
    from_unixtime(nullif(cast(json_query(c.executionDetails, 'lax $.notExecutableBefore') as double), 0)) as renewal_not_executable_before,
    from_unixtime(nullif(cast(json_query(c.executionDetails, 'lax $.executableUntil') as double), 0)) as renewal_executable_until,
    from_unixtime(nullif(cast(json_query(c.executionDetails, 'lax $.renewableUntil') as double), 0)) as renewal_renewable_until,
    cast(json_query(c.executionDetails, 'lax $.renewablePeriodBeforeExpiration') as bigint) as renewal_renewable_period_seconds_before_expiration,
    cast(json_query(c.executionDetails, 'lax $.maxPremiumInAsset') as uint256) as renewal_max_premium_in_asset,
    -- retrieve settlementDetails
    cast(json_query(c.settlementDetails, 'lax $.fee') as uint256) as settlement_fee,
    from_hex(json_query(c.settlementDetails, 'lax $.feeDestination' omit quotes)) as settlement_fee_destination,
    c.call_trace_address as trace_address,
    c.call_tx_hash as tx_hash
  from nexusmutual_ethereum.limitorders_call_executeorder c
    cross join unnest(c.poolAllocationRequests) as t(pool_allocation)
  where c.call_success
),

cover_edits as (
  select
    evt_block_time as block_time,
    buyer,
    productId as product_id,
    --amount, -- always null
    coverId as cover_id,
    originalCoverId as original_cover_id,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.cover_evt_coverbought
  where coverId <> originalCoverId
),

covers as (
  select
    block_time,
    block_number,
    'regular' as buy_type,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_period_seconds,
    pool_id,
    product_id,
    cover_owner,
    sum_assured,
    cover_asset,
    payment_asset,
    max_premium_in_asset,
    commission_ratio,
    commission_destination,
    cover_amount_in_asset,
    pool_allocation_skip,
    cover_ipfs_data,
    null as renewal_buyer,
    null as renewal_not_executable_before,
    null as renewal_executable_until,
    null as renewal_renewable_until,
    null as renewal_renewable_period_seconds_before_expiration,
    null as renewal_max_premium_in_asset,
    null as settlement_fee,
    null as settlement_fee_destination,
    trace_address,
    tx_hash
  from cover_sales
  union all
  select
    block_time,
    block_number,
    'renewal' as buy_type,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_period_seconds,
    pool_id,
    product_id,
    cover_owner,
    sum_assured,
    cover_asset,
    payment_asset,
    max_premium_in_asset,
    commission_ratio,
    commission_destination,
    cover_amount_in_asset,
    pool_allocation_skip,
    cover_ipfs_data,
    renewal_buyer,
    renewal_not_executable_before,
    renewal_executable_until,
    renewal_renewable_until,
    renewal_renewable_period_seconds_before_expiration,
    renewal_max_premium_in_asset,
    settlement_fee,
    settlement_fee_destination,
    trace_address,
    tx_hash
  from cover_renewals
),

covers_including_edits as (
  select
    c.block_time,
    c.block_number,
    case
      when c.buy_type = 'renewal' then 'renewal'
      when ceo.original_cover_id is not null then 'edit-original'
      when cen.cover_id is not null then 'edit-new'
      else 'regular'
    end as buy_type,
    c.cover_id,
    c.cover_start_time,
    -- update cover_end_time to block_time -1s on original covers (including renewals)
    case
      when ceo.original_cover_id is not null then date_add('second', -1, ceo.block_time)
      else c.cover_end_time
    end as cover_end_time,
    -- recalculate cover_period_seconds on original covers (including renewals)
    case
      when ceo.original_cover_id is not null then date_diff('second', c.cover_start_time, ceo.block_time)
      else c.cover_period_seconds
    end as cover_period_seconds,
    c.pool_id,
    c.product_id,
    c.cover_owner,
    c.sum_assured,
    c.cover_asset,
    c.payment_asset,
    c.max_premium_in_asset,
    c.commission_ratio,
    c.commission_destination,
    c.cover_amount_in_asset,
    c.pool_allocation_skip,
    c.cover_ipfs_data,
    c.renewal_buyer,
    c.renewal_not_executable_before,
    c.renewal_executable_until,
    c.renewal_renewable_until,
    c.renewal_renewable_period_seconds_before_expiration,
    c.renewal_max_premium_in_asset,
    c.settlement_fee,
    c.settlement_fee_destination,
    -- store for cross-reference
    if(ceo.original_cover_id is not null, c.cover_end_time, null) as original_cover_end_time,
    if(cen.cover_id is not null, cen.original_cover_id, null) as original_cover_id,
    if(ceo.original_cover_id is not null, ceo.cover_id, null) as new_cover_id,
    c.trace_address,
    c.tx_hash
  from covers c
    left join cover_edits ceo on c.cover_id = ceo.original_cover_id
    left join cover_edits cen on c.cover_id = cen.cover_id
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
    and contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4 -- proxy
),

cover_premiums as (
  select
    c.block_time,
    c.block_number,
    c.buy_type,
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    c.cover_period_seconds,
    c.pool_id,
    c.product_id,
    p.cover_amount / 100.0 as partial_cover_amount, -- partial_cover_amount_in_nxm
    p.premium / 1e18 as premium,
    p.premium_period_ratio,
    c.commission_ratio / 10000.0 as commission_ratio,
    --(c.commission_ratio / 10000.0) * p.premium / 1e18 as commission,
    p.premium / 1e18 / (1.0 - (c.commission_ratio / 10000.0)) - p.premium / 1e18 as commission,
    --(1.0 + (c.commission_ratio / 10000.0)) * (0.5 / (1.0 + (c.commission_ratio / 10000.0))) * p.premium / 1e18 as premium,
    --(1.0 + (c.commission_ratio / 10000.0)) * p.premium / 1e18 as premium_incl_commission,
    p.premium / 1e18 / (1.0 - (c.commission_ratio / 10000.0)) as premium_incl_commission,
    case c.cover_asset
      when 0 then 'ETH'
      when 1 then 'DAI'
      when 6 then 'USDC'
      when 7 then 'cbBTC'
      else 'NA'
    end as cover_asset,
    c.sum_assured / case c.cover_asset when 6 then 1e6 when 7 then 1e8 else 1e18 end as sum_assured,
    case c.payment_asset
      when 0 then 'ETH'
      when 1 then 'DAI'
      when 6 then 'USDC'
      when 7 then 'cbBTC'
      when 255 then 'NXM'
      else 'NA'
    end as premium_asset,
    c.cover_owner,
    c.commission_destination,
    c.cover_ipfs_data,
    c.renewal_renewable_until,
    c.original_cover_end_time,
    c.original_cover_id,
    c.new_cover_id,
    c.trace_address,
    c.tx_hash
  from covers_including_edits c
    inner join staking_product_premiums p on c.tx_hash = p.tx_hash and c.block_number = p.block_number
      and c.pool_id = p.pool_id and c.product_id = p.product_id
),

-- refund logic (only adjust original leg; keep new leg gross)
edit_refunds_by_pool as (
  select
    cp.cover_id as original_cover_id,
    cp.new_cover_id,
    cp.pool_id,
    cp.premium as premium_nxm_pool,
    greatest(
      0.0,
      date_diff('second', date_add('second', 1, cp.cover_end_time), cp.original_cover_end_time)
    ) * 1.0 / nullif(date_diff('second', cp.cover_start_time, cp.original_cover_end_time), 0) as unused_ratio,
    cp.premium * (
      greatest(
        0.0,
        date_diff('second', date_add('second', 1, cp.cover_end_time), cp.original_cover_end_time)
      ) * 1.0 / nullif(date_diff('second', cp.cover_start_time, cp.original_cover_end_time), 0)
    ) as refund_nxm_pool
  from cover_premiums cp
  where cp.buy_type = 'edit-original' and cp.original_cover_end_time is not null
),

cover_premiums_adjusted as (
  select
    cp.block_time,
    cp.block_number,
    cp.buy_type,
    cp.cover_id,
    cp.cover_start_time,
    cp.cover_end_time,
    cp.cover_period_seconds,
    cp.pool_id,
    cp.product_id,
    cp.partial_cover_amount, -- partial_cover_amount_in_nxm
    case
      when cp.buy_type = 'edit-original' then coalesce(cp.premium - er.refund_nxm_pool, cp.premium)
      when cp.buy_type = 'edit-new' then cp.premium
      else cp.premium
    end as premium,
    cp.premium_period_ratio,
    cp.commission_ratio,
    case
      when cp.buy_type = 'edit-new' then cp.commission
      else cp.commission
    end as commission,
    case
      when cp.buy_type = 'edit-original' then coalesce(cp.premium_incl_commission - er.refund_nxm_pool, cp.premium_incl_commission)
      when cp.buy_type = 'edit-new' then cp.premium_incl_commission
      else cp.premium_incl_commission
    end as premium_incl_commission,
    cp.cover_asset,
    cp.sum_assured,
    cp.premium_asset,
    cp.cover_owner,
    cp.commission_destination,
    cp.cover_ipfs_data,
    cp.renewal_renewable_until,
    cp.original_cover_end_time,
    cp.original_cover_id,
    cp.new_cover_id,
    cp.trace_address,
    cp.tx_hash
  from cover_premiums cp
    left join edit_refunds_by_pool er
      on cp.cover_id = er.original_cover_id
      and cp.pool_id = er.pool_id
      and cp.new_cover_id = er.new_cover_id
      and cp.buy_type = 'edit-original'
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
),

covers_v2 as (
  select
    cp.block_time,
    cp.block_number,
    cp.buy_type,
    cp.cover_id,
    cp.cover_start_time,
    cp.cover_end_time,
    cp.cover_period_seconds,
    cp.pool_id,
    cp.product_id,
    p.product_type,
    p.product_name,
    cp.partial_cover_amount, -- partial_cover_amount_in_nxm
    --cp.pool_premium,
    cp.cover_asset,
    cp.sum_assured,
    cp.premium_asset,
    cp.premium_period_ratio,
    cp.premium,
    cp.premium_incl_commission,
    cp.cover_owner,
    cp.commission,
    cp.commission_ratio,
    cp.commission_destination,
    cp.cover_ipfs_data,
    cp.renewal_renewable_until,
    cp.original_cover_end_time,
    cp.original_cover_id,
    cp.new_cover_id,
    cp.trace_address,
    cp.tx_hash
  from cover_premiums_adjusted cp
    left join products p on cp.product_id = p.product_id
),

covers_v1_migrated as (
  select
    cm.evt_block_time as block_time,
    cm.evt_block_number as block_number,
    'regular' as buy_type,
    cm.coverIdV2 as cover_id,
    cv1.cover_start_time,
    cv1.cover_end_time,
    date_diff('second', cv1.cover_start_time, cv1.cover_end_time) as cover_period_seconds,
    cv1.premium,
    cv1.premium_nxm,
    cv1.sum_assured,
    cv1.syndicate,
    cast(null as uint256) as product_id,
    cv1.product_name,
    cv1.product_type,
    cv1.cover_asset,
    cv1.premium_asset,
    cast(null as double) as premium_period_ratio,
    cm.newOwner as cover_owner,
    cast(null as double) as commission,
    cast(null as double) as commission_ratio,
    cast(null as varbinary) as commission_destination,
    cm.evt_index,
    cm.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.CoverMigrator_evt_CoverMigrated cm
    inner join query_3788367 cv1 -- covers v1
      on cm.coverIdV1 = cv1.cover_id
),

covers_combined as (
  select
    block_time,
    block_number,
    buy_type,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_period_seconds,
    pool_id as staking_pool_id,
    cast(pool_id as varchar) as staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    premium_asset,
    premium,
    premium as premium_nxm,
    premium_incl_commission,
    premium_period_ratio,
    sum_assured,
    partial_cover_amount, -- in NXM
    cover_owner,
    commission,
    commission_ratio,
    commission_destination,
    false as is_migrated,
    cover_ipfs_data,
    renewal_renewable_until,
    original_cover_end_time,
    original_cover_id,
    new_cover_id,
    trace_address,
    tx_hash
  from covers_v2
  union all
  select
    block_time,
    block_number,
    buy_type,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_period_seconds,
    cast(null as uint256) as staking_pool_id,
    syndicate as staking_pool,
    product_id,
    product_type,
    product_name,
    cover_asset,
    premium_asset,
    premium,
    premium_nxm,
    premium_nxm as premium_incl_commission,
    premium_period_ratio,
    sum_assured,
    sum_assured as partial_cover_amount, -- No partial covers in v1 migrated covers
    cover_owner,
    commission,
    commission_ratio,
    commission_destination,
    true as is_migrated,
    null as cover_ipfs_data,
    null as renewal_renewable_until,
    null as original_cover_end_time,
    null as original_cover_id,
    null as new_cover_id,
    null as trace_address,
    tx_hash
  from covers_v1_migrated
)

select
  block_time,
  date_trunc('day', block_time) as block_date,
  block_number,
  buy_type,
  cover_id,
  cover_start_time,
  cover_end_time,
  cover_period_seconds,
  date_trunc('day', cover_start_time) as cover_start_date,
  date_trunc('day', cover_end_time) as cover_end_date,
  staking_pool_id,
  staking_pool,
  cast(product_id as int) as product_id,
  product_type,
  product_name,
  cover_asset,
  sum_assured,
  partial_cover_amount, -- in NXM
  premium_asset,
  premium,
  premium_nxm,
  premium_incl_commission,
  premium_period_ratio,
  cover_owner,
  commission,
  commission_ratio,
  commission_destination,
  is_migrated,
  cover_ipfs_data,
  renewal_renewable_until,
  original_cover_end_time,
  original_cover_id,
  new_cover_id,
  trace_address,
  tx_hash
from covers_combined
--order by cover_id desc
