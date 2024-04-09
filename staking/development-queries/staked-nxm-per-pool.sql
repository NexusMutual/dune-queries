with

staking_pools as (
  select distinct
    pool_id,
    pool_address,
    pool_name
  from query_3597103
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
    -1 * cast((w.amountStakeWithdrawn) as double) as amount, -- + w.amountRewardsWithdrawn
    w.user,
    w.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_Withdraw w
    inner join staking_nft_mints m on w.tokenId = m.token_id

  union all

  select
    'stake burn - pool' as flow_type,
    eb.evt_block_time as block_time,
    cb.poolId as pool_id,
    cast(null as uint256) as token_id,
    cast(null as uint256) as tranche_id,
    cast(null as uint256) as new_tranche_id,
    -1 * cast(eb.amount as double) as amount,
    cast(null as varbinary) as user,
    eb.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeBurned eb
    inner join nexusmutual_ethereum.TokenController_call_burnStakedNXM cb on eb.evt_tx_hash = cb.call_tx_hash
  where cb.call_success

  union all

  select
    'stake burn - cover' as flow_type,
    cb.call_block_time as block_time,
    c.pool_id,
    cast(null as uint256) as token_id,
    cast(null as uint256) as tranche_id,
    cast(null as uint256) as new_tranche_id,
    -1 * cast(cb.payoutAmountInAsset as double) as amount,
    cb.output_0 as user,
    cb.call_tx_hash
  from nexusmutual_ethereum.Cover_call_burnStake cb
    inner join (
      select distinct cover_id, pool_id from covers
    ) c on cb.coverId = c.cover_id
  where cb.call_success
),

staked_nxm_history_with_rewards as (
  select
    'deposit' as flow_type,
    sd.evt_block_time as block_time,
    m.pool_id,
    sd.tokenId as token_id,
    sd.trancheId as tranche_id,
    cast(null as uint256) as new_tranche_id,
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
    -1 * cast((w.amountStakeWithdrawn + w.amountRewardsWithdrawn) as double) as amount,
    w.user,
    w.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_Withdraw w
    inner join staking_nft_mints m on w.tokenId = m.token_id

  union all

  select
    'stake burn - pool' as flow_type,
    eb.evt_block_time as block_time,
    cb.poolId as pool_id,
    cast(null as uint256) as token_id,
    cast(null as uint256) as tranche_id,
    cast(null as uint256) as new_tranche_id,
    -1 * cast(eb.amount as double) as amount,
    cast(null as varbinary) as user,
    eb.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeBurned eb
    inner join nexusmutual_ethereum.TokenController_call_burnStakedNXM cb on eb.evt_tx_hash = cb.call_tx_hash
  where cb.call_success

  union all

  select
    'stake burn - cover' as flow_type,
    cb.call_block_time as block_time,
    c.pool_id,
    cast(null as uint256) as token_id,
    cast(null as uint256) as tranche_id,
    cast(null as uint256) as new_tranche_id,
    -1 * cast(cb.payoutAmountInAsset as double) as amount,
    cb.output_0 as user,
    cb.call_tx_hash
  from nexusmutual_ethereum.Cover_call_burnStake cb
    inner join (
      select distinct cover_id, pool_id from covers
    ) c on cb.coverId = c.cover_id
  where cb.call_success
),

staking_rewards as (
  select
    mr.call_block_time as block_time,
    mr.poolId as pool_id,
    c.product_id,
    c.cover_id,
    mr.amount as reward_amount_total,
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
  group by 1
),

staked_nxm_per_pool_bar_withdrawn_rewards as (
  select
    pool_id,
    sum(amount) / 1e18 as amount_minus_withdrawn_rewards
  from staked_nxm_history_with_rewards
  group by 1
),

staked_nxm_rewards_per_pool as (
  select
    pool_id,
    sum(reward_amount_total) / 1e18 as reward_amount
  from staking_rewards
  group by 1
)

select
  sp.pool_id,
  sp.pool_name,
  t.amount,
  tr.amount_minus_withdrawn_rewards,
  r.reward_amount
from staking_pools sp
  left join staked_nxm_per_pool t on sp.pool_id = t.pool_id
  left join staked_nxm_per_pool_bar_withdrawn_rewards tr on sp.pool_id = tr.pool_id
  left join staked_nxm_rewards_per_pool r on sp.pool_id = r.pool_id
order by 1
