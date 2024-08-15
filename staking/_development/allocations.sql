with

request_allocations as (
  select
    call_block_time as block_time,
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
    call_trace_address,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_number, call_tx_hash, output_allocationId order by call_trace_address desc) as rn
  from nexusmutual_ethereum.StakingPool_call_requestAllocation
  where call_success
),

covers as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    staking_pool_id,
    product_id
  from nexusmutual_ethereum.covers_v2
  --from query_3788370 -- covers v2 base (fallback) query
  where cover_end_date >= current_date
),

allocations as (
  select
    c.staking_pool_id,
    c.cover_id,
    c.product_id,
    a.allocation_id,
    a.block_time,
    a.period,
    a.grace_period,
    a.amount / 1e18 as amount,
    a.premium / 1e18 as premium
  from request_allocations a
    inner join covers c on a.cover_id = c.cover_id and a.product_id = c.product_id
  where a.rn = 1
    and a.cover_id > 0
)

select
  staking_pool_id,
  sum(amount) as amount,
  sum(premium) as premium
from allocations
group by 1
order by 1
