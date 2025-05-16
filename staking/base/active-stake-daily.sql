with

staking_pools as (
  select
    pool_id,
    pool_address
  --from nexusmutual_ethereum.staking_pools_list
  from query_5128062 -- staking pools list - base query
),

active_stake_updated as (
  select
    sp.pool_id,
    sp.pool_address,
    asu.evt_block_time as block_time,
    asu.evt_block_date as block_date,
    asu.evt_block_number as block_number,
    asu.activeStake / 1e18 as active_stake,
    asu.stakeSharesSupply as stake_shares_supply,
    asu.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_ActiveStakeUpdated asu
    inner join staking_pools sp on asu.contract_address = sp.pool_address
),

active_stake_updated_daily as (
  select
    pool_id,
    pool_address,
    block_date,
    max_by(active_stake, block_date) as active_stake,
    max_by(stake_shares_supply, block_date) as stake_shares_supply,
    max_by(tx_hash, block_date) as tx_hash
  from active_stake_updated
  group by 1, 2, 3
)

select
  pool_id,
  pool_address,
  block_date,
  active_stake,
  stake_shares_supply,
  tx_hash
from active_stake_updated_daily
--order by 1, 2, 3
