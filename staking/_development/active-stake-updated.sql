with

staking_pools as (
  select distinct
    pool_id,
    pool_address
  --from query_3859935 -- staking pools base (fallback) query
  from query_4167546 -- staking pools - spell de-duped
),

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

active_stake_updated as (
  select
    contract_address,
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
from active_stake_updated asu
  inner join staking_pools sp on asu.contract_address = sp.pool_address
  left join staking_pool_names spn on sp.pool_id = spn.pool_id
where asu.rn = 1
order by 1
