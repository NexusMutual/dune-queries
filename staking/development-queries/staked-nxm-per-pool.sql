with

staking_pools as (
  select distinct
    pool_id,
    pool_address,
    pool_name,
    manager
  from query_3597103
),

staking_pool_products_all_days as (
  select
    sp.pool_id,
    sp.product_id,
    s.block_date
  from query_3597103 sp
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

staked_nxm_history as (
  select
    'deposit' as flow_type,
    sd.evt_block_time as block_time,
    m.pool_id,
    sd.tokenId as token_id,
    sd.trancheId as tranche_id,
    cast(null as uint256) as new_tranche_id,
    if(from_unixtime(91.0 * 86400.0 * cast(sd.trancheId + 1 as double)) < date_trunc('day', now()), false, true) as is_active,
    cast(sd.amount as double) as amount,
    sd.user,
    sd.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeDeposited sd
    inner join staking_nft_mints m on sd.tokenId = m.token_id
  
  union all

  select
    'deposit extended' as flow_type,
    de.evt_block_time as block_time,
    m.pool_id,
    de.tokenId as token_id,
    de.initialTrancheId as tranche_id,
    de.newTrancheId as new_tranche_id,
    if(from_unixtime(91.0 * 86400.0 * cast(de.newTrancheId + 1 as double)) < date_trunc('day', now()), false, true) as is_active,
    cast(de.topUpAmount as double) as amount,
    de.user,
    de.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_DepositExtended de
    inner join staking_nft_mints m on de.tokenId = m.token_id

  union all

  select
    'withdraw' as flow_type,
    w.evt_block_time as block_time,
    m.pool_id,
    w.tokenId as token_id,
    w.tranche as tranche_id,
    cast(null as uint256) as new_tranche_id,
    if(from_unixtime(91.0 * 86400.0 * cast(w.tranche + 1 as double)) < date_trunc('day', now()), false, true) as is_active,
    -1 * cast((w.amountStakeWithdrawn) as double) as amount,
    w.user,
    w.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_Withdraw w
    inner join staking_nft_mints m on w.tokenId = m.token_id

  union all

  select
    'stake burn' as flow_type,
    eb.evt_block_time as block_time,
    cb.poolId as pool_id,
    cast(null as uint256) as token_id,
    cast(null as uint256) as tranche_id,
    cast(null as uint256) as new_tranche_id,
    false as is_active, -- ???
    -1 * cast(eb.amount as double) as amount,
    cast(null as varbinary) as user,
    eb.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeBurned eb
    inner join nexusmutual_ethereum.TokenController_call_burnStakedNXM cb on eb.evt_tx_hash = cb.call_tx_hash
  where cb.call_success
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

staked_nxm_per_pool as (
  select
    pool_id,
    sum(amount) / 1e18 as amount
  from staked_nxm_history
  where is_active
  group by 1
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
  t.amount
  --dr.reward_amount_current_total,
  --r.reward_amount_expected_total
from staking_pools sp
  left join staked_nxm_per_pool t on sp.pool_id = t.pool_id
  left join staked_nxm_per_pool_bar_withdrawn_rewards tr on sp.pool_id = tr.pool_id
  left join staked_nxm_daily_rewards_per_pool dr on sp.pool_id = dr.pool_id --and dr.rn = 1
  left join staked_nxm_rewards_per_pool r on sp.pool_id = r.pool_id
order by 1
