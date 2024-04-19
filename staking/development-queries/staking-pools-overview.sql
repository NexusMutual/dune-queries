with

staking_pool_products as (
  select
    pool_id,
    pool_address,
    product_id,
    coalesce(target_weight, initial_weight) as target_weight
  from query_3597103 -- staking pools base data
),

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    sp.pool_name,
    sp.pool_created_time,
    if(sp.is_private_pool, 'Private', 'Public') as pool_type,
    sp.manager,
    sp.initial_pool_fee / 100.00 as initial_pool_fee,
    sp.current_pool_fee / 100.00 as current_management_fee,
    sp.max_management_fee / 100.00 as max_management_fee,
    spp.total_weight / 100.00 as leverage,
    spp.product_count
  from query_3597103 sp -- staking pools base data
    inner join (
      select
        pool_id,
        count(distinct product_id) filter (where target_weight > 0) as product_count,
        sum(target_weight) as total_weight
      from staking_pool_products
      group by 1
    ) spp on sp.pool_id = spp.pool_id
),

staking_pool_fee_history as (
  select
    sp.pool_id,
    sp.pool_address,
    t.start_time,
    coalesce(
      date_add('second', -1, lead(t.start_time) over (partition by t.pool_address order by t.start_time)),
      now()
    ) as end_time,
    t.pool_fee
  from staking_pools sp
    inner join (
      select
        pool_address,
        pool_created_time as start_time,
        initial_pool_fee as pool_fee
      from staking_pools
      union all
      select
        contract_address as pool_address,
        evt_block_time as start_time,
        newFee / 100.00 as pool_fee
      from nexusmutual_ethereum.StakingPool_evt_PoolFeeChanged
    ) t on sp.pool_address = t.pool_address
),

staking_pool_products_all_days as (
  select
    sp.pool_id,
    sp.product_id,
    s.block_date
  from query_3597103 sp -- staking pools base data
    cross join unnest (
      sequence(
        cast(date_trunc('day', sp.product_added_time) as timestamp),
        cast(date_trunc('day', now()) as timestamp),
        interval '1' day
      )
    ) as s(block_date)
),

staking_nft_mints as (
  select
    call_block_time as block_time,
    to as minter,
    poolId as pool_id,
    output_id as token_id,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingNFT_call_mint
  where call_success
),

covers as (
  select
    c.call_block_time as block_time,
    c.call_block_number as block_number,
    c.output_coverId as cover_id,
    c.call_block_time as cover_start_time,
    date_add('second', cast(json_query(c.params, 'lax $.period') as int), c.call_block_time) as cover_end_time,
    cast(json_query(c.params, 'lax $.period') as int) as cover_period_seconds,
    cast(json_query(t.pool_allocation, 'lax $.poolId') as uint256) as pool_id,
    cast(json_query(c.params, 'lax $.productId') as uint256) as product_id,
    json_query(c.params, 'lax $.owner' omit quotes) as owner,
    cast(json_query(c.params, 'lax $.amount') as uint256) as cover_amount,
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
    call_block_time as premium_start_time,
    call_block_number as block_number,
    poolId as pool_id,
    productId as product_id,
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
    c.cover_start_time,
    c.pool_id,
    c.product_id,
    c.cover_id,
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
    inner join staking_pool_fee_history h on p.pool_id = h.pool_id
      and p.premium_start_time between h.start_time and h.end_time
),

staked_nxm_per_pool as (
  select
    sp.pool_id,
    t.pool_address,
    sum(t.amount) as total_nxm_staked
  from (
      select
        pool_address,
        sum(total_amount) as amount
      from query_3619534 -- staking pools deposit extensions
      where is_active
      group by 1
      union all
      select
        pool_address,
        sum(amount) as amount
      from query_3609519 -- staking pool history
      where is_active
        --and flow_type in ('withdraw', 'stake burn')
        and flow_type = 'withdraw' -- burn TBD
      group by 1
    ) t
    join staking_pools sp on t.pool_address = sp.pool_address
  group by 1,2
),

staked_nxm_allocated as (
  select
    w.pool_id,
    sum(w.target_weight * s.total_nxm_staked) / 100.00 as total_nxm_allocated
  from staking_pool_products w
    inner join staked_nxm_per_pool s on w.pool_id = s.pool_id
  group by 1
),

staking_rewards as (
  select
    mr.call_block_time as block_time,
    mr.call_block_number as block_number,
    date_trunc('day', mr.call_block_time) as block_date,
    mr.poolId as pool_id,
    c.product_id,
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    date_diff('day', c.cover_start_time, if(c.cover_end_time > now(), now(), c.cover_end_time)) as cover_period_days,
    mr.amount as reward_amount_expected_total,
    mr.amount * 86400.0 / c.cover_period_seconds as reward_amount_per_day,
    mr.call_tx_hash as tx_hash
  from nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards mr
    inner join covers c on mr.call_tx_hash = c.tx_hash and mr.call_block_number = c.block_number
  where mr.call_success
    and mr.poolId = c.pool_id
    and (c.call_trace_address is null
      or slice(mr.call_trace_address, 1, cardinality(c.call_trace_address)) = c.call_trace_address)
),

staked_nxm_daily_rewards_per_pool as (
  select
    d.pool_id,
    sum(r.reward_amount_per_day) / 1e18 as reward_amount_current_total
    --sum(r.reward_amount_per_day / 1e18) over (partition by d.pool_id order by d.block_time) as reward_amount_current_total,
    --row_number() over (partition by d.pool_id order by d.block_time desc) as rn
  from staking_pool_products_all_days d
    inner join staking_rewards r on d.pool_id = r.pool_id and d.product_id = r.product_id
  where d.block_date < current_date
  group by 1
),

staked_nxm_rewards_per_pool as (
  select
    pool_id,
    sum(reward_amount_per_day * cover_period_days) / 1e18 as reward_amount_current_total,
    sum(reward_amount_expected_total) / 1e18 as reward_amount_expected_total
  from staking_rewards
  group by 1
)

select
  sp.pool_id,
  sp.pool_name,
  sp.manager,
  sp.pool_type,
  sp.current_management_fee,
  sp.max_management_fee,
  coalesce(t.total_nxm_staked, 0) as total_nxm_staked,
  coalesce(a.total_nxm_allocated, 0) as total_nxm_allocated,
  sp.leverage,
  sp.product_count,
  r.reward_amount_current_total,
  r.reward_amount_expected_total
from staking_pools sp
  left join staked_nxm_per_pool t on sp.pool_id = t.pool_id
  left join staked_nxm_allocated a on sp.pool_id = a.pool_id
  left join staked_nxm_rewards_per_pool r on sp.pool_id = r.pool_id
order by 1
