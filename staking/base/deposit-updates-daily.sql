with

staking_pools as (
  select
    pool_id,
    pool_address
  --from nexusmutual_ethereum.staking_pools_list
  from query_5128062 -- staking pools list - base query
),

active_stake_updates as (
  select
    sp.pool_id,
    sp.pool_address,
    asu.evt_block_time as block_time,
    asu.evt_block_date as block_date,
    asu.activeStake / 1e18 as active_stake,
    asu.evt_index,
    asu.evt_tx_hash as tx_hash,
    row_number() over (partition by asu.evt_block_time, asu.evt_tx_hash order by asu.evt_index desc) as rn
  from nexusmutual_ethereum.StakingPool_evt_ActiveStakeUpdated asu
    inner join staking_pools sp on asu.contract_address = sp.pool_address
),

deposit_updated as (
  select
    sp.pool_id,
    sp.pool_address,
    du.evt_block_time as block_time,
    du.evt_block_date as block_date,
    du.trancheId as tranche_id,
    du.tokenId as token_id,
    du.stakeShares as stake_shares,
    du.stakeSharesSupply as stake_shares_supply,
    du.evt_index,
    du.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_DepositUpdated du
    inner join staking_pools sp on du.contract_address = sp.pool_address
),

deposit_updated_daily as (
  select
    du.block_time,
    du.block_date,
    du.pool_id,
    du.pool_address,
    du.tranche_id,
    du.token_id,
    asu.active_stake,
    du.stake_shares,
    du.stake_shares_supply,
    du.tx_hash,
    asu.active_stake * du.stake_shares / du.stake_shares_supply as token_stake,
    row_number() over (partition by du.pool_id, du.tranche_id, du.token_id order by du.block_time desc) as rn
  from deposit_updated du
    inner join active_stake_updates asu
      on du.pool_id = asu.pool_id
      and du.block_time = asu.block_time
      and du.tx_hash = asu.tx_hash
      and du.evt_index < asu.evt_index
      and asu.rn = 1
)

select
  block_time,
  block_date,
  pool_id,
  pool_address,
  tranche_id,
  token_id,
  active_stake,
  stake_shares,
  stake_shares_supply,
  token_stake,
  tx_hash,
  rn
from deposit_updated_daily
--where rn = 1
--order by pool_id, tranche_id, token_id, block_time
