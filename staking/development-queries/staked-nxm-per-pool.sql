with

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
    evt_block_time as block_time,
    user,
    tokenId as token_id,
    trancheId as tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(amount as double) as amount,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeDeposited
  
  union all

  select
    'deposit ext' as flow_type,
    evt_block_time as block_time,
    user,
    tokenId as token_id,
    initialTrancheId as tranche_id,
    newTrancheId as new_tranche_id,
    cast(topUpAmount as double) as amount,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_DepositExtended

  union all

  select
    'withdraw' as flow_type,
    evt_block_time as block_time,
    user,
    tokenId as token_id,
    tranche as tranche_id,
    cast(null as uint256) as new_tranche_id,
    -1 * cast((amountStakeWithdrawn + amountRewardsWithdrawn) as double) as amount,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_Withdraw
),

staked_nxm_totals as (
  select
    token_id,
    sum(amount) as amount
  from staked_nxm_history
  group by 1
)

select
  m.pool_id,
  sum(t.amount) / 1e18 as amount
from staking_nft_mints as m
  join staked_nxm_totals as t on m.token_id = t.token_id
group by 1
order by 1
