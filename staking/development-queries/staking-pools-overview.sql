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
    if(sp.is_private_pool, 'Private', 'Public') as pool_type,
    sp.manager,
    sp.initial_pool_fee / 100.00 as current_management_fee,
    sp.max_management_fee / 100.00 as max_management_fee,
    spp.total_weight / 100.00 as leverage
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

covers as (
  select
    call_block_time as block_time,
    output_coverId as cover_id,
    cast(json_query(poolAllocationRequests[1], 'lax $.poolId') as uint256) as pool_id,
    cast(json_query(params, 'lax $.productId') as uint256) as product_id,
    cast(json_query(params, 'lax $.period') as uint256) as period,
    call_trace_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.Cover_call_buyCover
  where call_success
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

staked_nxm_per_pool as (
  select
    pool_address,
    sum(amount) as total_nxm_staked
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
  group by 1
),

staked_nxm_allocated as (
  select
    w.pool_address,
    sum(w.target_weight * s.total_nxm_staked) / 100.00 as total_nxm_allocated
  from staking_pool_products w
    inner join staked_nxm_per_pool s on w.pool_address = s.pool_address
  group by 1
),

staking_rewards as (
  select
    mr.call_block_time as block_time,
    date_trunc('day', mr.call_block_time) as block_date,
    mr.poolId as pool_id,
    c.product_id,
    c.cover_id,
    mr.amount as reward_amount_expected_total,
    mr.amount * 86400.0 / c.period as reward_amount_per_day,
    mr.call_tx_hash as tx_hash
  from nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards mr
    inner join covers c on mr.call_tx_hash = c.tx_hash
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
  sp.product_count
  --dr.reward_amount_current_total,
  --r.reward_amount_expected_total
from staking_pools sp
  left join staked_nxm_per_pool t on sp.pool_address = t.pool_address
  left join staked_nxm_allocated a on sp.pool_address = a.pool_address
  left join staked_nxm_daily_rewards_per_pool dr on sp.pool_id = dr.pool_id --and dr.rn = 1
  left join staked_nxm_rewards_per_pool r on sp.pool_id = r.pool_id
order by 1
