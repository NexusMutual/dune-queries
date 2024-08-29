with

/*
staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),
*/

staking_pool_base as (
  select
    sp.pool_id,
    sp.pool_address,
    --spn.pool_name,
    sp.manager_address,
    sp.manager_ens,
    sp.manager,
    sp.is_private_pool,
    sp.initial_pool_fee,
    sp.current_pool_fee,
    sp.max_management_fee,
    sp.product_id,
    sp.initial_price,
    sp.target_price,
    sp.initial_weight,
    sp.target_weight,
    sp.pool_created_time,
    sp.product_added_time
  --from query_3859935 sp -- staking pools base (fallback) query
  from nexusmutual_ethereum.staking_pools sp
    --left join staking_pool_names spn on sp.pool_id = spn.pool_id
),

staking_pool_products as (
  select
    pool_id,
    pool_address,
    product_id,
    coalesce(target_weight, initial_weight) as target_weight
  from staking_pool_base
),

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    --sp.pool_name,
    sp.pool_created_time,
    if(sp.is_private_pool, 'Private', 'Public') as pool_type,
    sp.manager,
    sp.initial_pool_fee / 100.00 as initial_pool_fee,
    sp.current_pool_fee / 100.00 as current_management_fee,
    sp.max_management_fee / 100.00 as max_management_fee,
    spp.total_weight / 100.00 as leverage,
    spp.product_count
  from staking_pool_base sp
    inner join (
      select
        pool_id,
        count(distinct product_id) filter (where target_weight > 0) as product_count,
        sum(target_weight) as total_weight
      from staking_pool_products
      group by 1
    ) spp on sp.pool_id = spp.pool_id
),

/*
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

allocation_requests as (
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
    row_number() over (partition by call_block_number, call_tx_hash order by call_trace_address desc) as rn
  from nexusmutual_ethereum.StakingPool_call_requestAllocation
  where call_success
),
*/

staked_nxm_per_pool as (
  select
    sp.pool_id,
    t.pool_address,
    t.block_date,
    sum(t.amount) over (partition by t.pool_address order by t.block_date) as total_nxm_staked,
    row_number() over (partition by t.pool_address order by t.block_date desc) as rn
  from (
      select
        pool_address,
        date_trunc('day', block_time) as block_date,
        sum(total_amount) as amount
      --from query_3619534 -- staking deposit extensions base query -- can't be used bc of "Error: This query has too many stages and is too complex to execute at once"
      from nexusmutual_ethereum.staking_deposit_extensions
      where is_active
      group by 1, 2
      union all
      select
        pool_address,
        date_trunc('day', block_time) as block_date,
        sum(amount) as amount
      --from query_3609519 -- staking events
      from nexusmutual_ethereum.staking_events
      where is_active
        --and flow_type in ('withdraw', 'stake burn')
        and flow_type = 'withdraw' -- burn TBD
      group by 1, 2
    ) t
    join staking_pools sp on t.pool_address = sp.pool_address
),

staked_nxm_allocated as (
  select
    w.pool_id,
    sum(w.target_weight * s.total_nxm_staked) / 100.00 as total_nxm_allocated
  from staking_pool_products w
    inner join staked_nxm_per_pool s on w.pool_id = s.pool_id
  where s.rn = 1
  group by 1
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

covers as (
  select
    *,
    date_diff('second', cover_start_time, cover_end_time) as cover_period_seconds
  from nexusmutual_ethereum.covers_v2
  --from query_3788370 -- covers v2 base (fallback) query
),

/*
cover_premiums as (
  select
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    c.staking_pool_id,
    c.product_id,
    c.premium,
    c.commission as cover_commision,
    c.premium * 0.5 as commission,
    c.commission_ratio * c.premium * 0.5 as commission_distibutor_fee,
    c.premium * 0.5 * h.pool_fee as pool_manager_commission,
    c.premium * 0.5 * (1 - h.pool_fee) as staker_commission,
    c.premium_period_ratio * c.premium * 0.5 as commission_emitted,
    c.premium_period_ratio * c.premium * 0.5 * h.pool_fee as pool_manager_commission_emitted,
    c.premium_period_ratio * c.premium * 0.5 * (1 - h.pool_fee) as staker_commission_emitted
  from covers c
    inner join staking_pool_fee_history h on c.staking_pool_id = h.pool_id
      and c.cover_start_time between h.start_time and h.end_time
),

cover_premium_commissions as (
  select
    staking_pool_id,
    sum(commission) as total_commission,
    sum(commission_emitted) as total_commission_emitted,
    sum(commission_distibutor_fee) as pool_distributor_commission,
    sum(pool_manager_commission) as pool_manager_commission,
    sum(pool_manager_commission_emitted) as pool_manager_commission_emitted,
    sum(staker_commission) as staker_commission,
    sum(staker_commission_emitted) as staker_commission_emitted
  from cover_premiums
  group by 1
),
*/

staking_rewards as (
  select
    mr.call_block_time as block_time,
    mr.call_block_number as block_number,
    date_trunc('day', mr.call_block_time) as block_date,
    mr.poolId as pool_id,
    c.product_id,
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    --date_diff('day', c.cover_start_time, if(c.cover_end_time > now(), now(), c.cover_end_time)) as elapsed_cover_period_days,
    --if(c.cover_end_time < now(), 0, date_diff('second', c.cover_start_time, now())) as elapsed_cover_period_seconds,
    --if(c.cover_end_time < now(), 0, mr.amount / c.cover_period_seconds) as active_reward_amount_per_second,
    mr.amount / 1e18 as reward_amount_expected_total,
    mr.amount / c.cover_period_seconds / 1e18 as reward_amount_per_second,
    mr.amount / c.cover_period_seconds * 86400.0 / 1e18 as reward_amount_per_day,
    mr.call_tx_hash as tx_hash
  from (
      select call_block_time, call_block_number, poolId, amount, call_trace_address, call_tx_hash
      from nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards
      where call_success
      union all
      select call_block_time, call_block_number, poolId, amount, call_trace_address, call_tx_hash
      from nexusmutual_ethereum.TokenController2_call_mintStakingPoolNXMRewards
      where call_success
    ) mr
    inner join covers c on mr.call_tx_hash = c.tx_hash and mr.call_block_number = c.block_number
  where mr.poolId = c.staking_pool_id
    and (c.trace_address is null
      or slice(mr.call_trace_address, 1, cardinality(c.trace_address)) = c.trace_address)
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    s.block_date
  from staking_pools sp
    cross join unnest (
      sequence(
        cast(date_trunc('day', sp.pool_created_time) as timestamp),
        cast(date_trunc('day', now()) as timestamp),
        interval '1' day
      )
    ) as s(block_date)
),

staked_nxm_daily_rewards_per_pool as (
  select distinct
    d.pool_id,
    d.block_date,
    sum(r.reward_amount_per_day) over (partition by d.pool_id order by d.block_date) as reward_amount_current_total,
    row_number() over (partition by d.pool_id order by d.block_date desc) as rn
  from staking_pool_day_sequence d
    left join staking_rewards r on d.pool_id = r.pool_id and d.block_date between r.cover_start_date and date_add('day', -1, r.cover_end_date)
  where d.block_date < current_date
),

staked_nxm_rewards_per_pool as (
  select
    pool_id,
    --sum(reward_amount_per_day * elapsed_cover_period_days) / 1e18 as reward_amount_current_total,
    --sum(active_reward_amount_per_second * elapsed_cover_period_seconds) / 1e18 as reward_amount_current_total_2,
    sum(reward_amount_expected_total) as reward_amount_expected_total
  from staking_rewards
  group by 1
)

select
  sp.pool_id,
  --sp.pool_name,
  --sp.manager,
  --sp.pool_type,
  sp.current_management_fee,
  sp.max_management_fee,
  sp.leverage,
  sp.product_count,
  --if(r.reward_amount_current_total_2=0, 0, r.reward_amount_current_total_2 * 365 / t.total_nxm_staked) as apy,
  coalesce(t.total_nxm_staked, 0) as total_nxm_staked,
  coalesce(a.total_nxm_allocated, 0) as total_nxm_allocated,
  dr.reward_amount_current_total,
  r.reward_amount_expected_total
  /*
  c.pool_manager_commission + c.pool_distributor_commission + c.staker_commission as total_commission,
  c.pool_distributor_commission,
  c.staker_commission_emitted,
  c.staker_commission - c.staker_commission_emitted as future_staker_commission,
  c.pool_manager_commission_emitted,
  c.pool_manager_commission - c.pool_manager_commission_emitted as future_pool_manager_commission
  */
from staking_pools sp
  left join staked_nxm_per_pool t on sp.pool_id = t.pool_id and t.rn = 1
  left join staked_nxm_allocated a on sp.pool_id = a.pool_id
  left join staked_nxm_daily_rewards_per_pool dr on sp.pool_id = dr.pool_id and dr.rn = 1
  left join staked_nxm_rewards_per_pool r on sp.pool_id = r.pool_id
  --left join cover_premium_commissions c on sp.pool_id = c.staking_pool_id
order by 1
