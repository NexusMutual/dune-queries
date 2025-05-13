with

staking_pools as (
  select
    evt_block_time as block_time,
    poolId as pool_id,
    stakingPoolAddress as pool_address,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPoolFactory_evt_StakingPoolCreated
),

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

active_stake_updated as (
  select
    contract_address as pool_address,
    evt_block_time as block_time,
    evt_block_date as block_date,
    evt_block_number as block_number,
    activeStake as active_stake,
    stakeSharesSupply as stake_shares_supply,
    evt_tx_hash as tx_hash,
    row_number() over (partition by contract_address order by evt_block_time desc) as rn
  from nexusmutual_ethereum.stakingpool_evt_activestakeupdated
)

select
  sp.pool_id,
  sp.pool_address,
  spn.pool_name,
  asu.block_time,
  asu.block_date,
  asu.block_number,
  asu.active_stake / 1e18 as active_stake,
  asu.stake_shares_supply,
  asu.tx_hash
from staking_pools sp
  inner join active_stake_updated asu on sp.pool_address = asu.contract_address
  left join staking_pool_names spn on sp.pool_id = spn.pool_id
where asu.rn = 1
order by 1
