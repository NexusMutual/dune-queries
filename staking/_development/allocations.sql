with

request_allocations as (
  select
    call_block_time as allocation_time,
    call_block_number as block_number,
    output_allocationId as allocation_id,
    output_premium as premium,
    amount,
    cast(json_query(request, 'lax $.coverId') as uint256) as cover_id,
    cast(json_query(request, 'lax $.productId') as uint256) as product_id,
    cast(json_query(request, 'lax $.period') as bigint) as period,
    cast(json_query(request, 'lax $.gracePeriod') as bigint) as grace_period,
    cast(json_query(request, 'lax $.useFixedPrice') as boolean) as use_fixed_price,
    cast(json_query(request, 'lax $.rewardRatio') as bigint) as reward_ratio,
    cast(json_query(request, 'lax $.globalMinPrice') as bigint) as global_min_price,
    cast(json_query(request, 'lax $.globalCapacityRatio') as bigint) as global_capacity_ratio,
    cast(json_query(request, 'lax $.capacityReductionRatio') as bigint) as capacity_reduction_ratio,
    cast(json_query(request, 'lax $.previousStart') as bigint) as previous_start,
    cast(json_query(request, 'lax $.previousExpiration') as bigint) as previous_expiration,
    call_trace_address,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_number, call_tx_hash, output_allocationId order by call_trace_address desc) as rn
  from nexusmutual_ethereum.StakingPool_call_requestAllocation
  where call_success
),

covers_per_active_bucket as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    staking_pool_id,
    product_id,
    cover_asset,
    sum_assured,
    partial_cover_amount as nxm_cover_amount_per_pool
  from query_4599092 -- covers v2 - base root (fallback query)
  where cover_end_time >= from_unixtime(28.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), now()) / 28.0) as double)) -- bucket start date
),

allocations as (
  select
    c.staking_pool_id,
    c.cover_id,
    c.product_id,
    a.allocation_id,
    a.allocation_time,
    a.period / 86400.0 as period,
    a.grace_period / 86400.0 as grace_period,
    a.use_fixed_price,
    a.reward_ratio,
    a.global_min_price,
    a.global_capacity_ratio,
    a.capacity_reduction_ratio,
    a.previous_start,
    a.previous_expiration,
    c.cover_asset,
    c.sum_assured,
    c.nxm_cover_amount_per_pool,
    a.amount / 1e18 as amount,
    a.premium / 1e18 as premium
  from request_allocations a
    inner join covers_per_active_bucket c on a.cover_id = c.cover_id and a.product_id = c.product_id
  where a.rn = 1
    --and a.cover_id > 0
)

select
  staking_pool_id,
  product_id,
  --period,
  --grace_period,
  --use_fixed_price,
  --reward_ratio,
  --global_min_price,
  --global_capacity_ratio,
  --capacity_reduction_ratio,
  cover_id,
  cover_asset,
  sum_assured,
  nxm_cover_amount_per_pool,
  allocation_id,
  allocation_time,
  amount,
  premium
from allocations
where staking_pool_id = 2
  and product_id = 130
order by cover_id, allocation_time

/*
select
  staking_pool_id,
  product_id,
  sum(amount) as amount,
  sum(premium) as premium
from allocations
group by 1, 2
order by 1
*/
