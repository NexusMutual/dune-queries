with

staking_pools as (
  select distinct
    pool_id,
    pool_address,
    pool_name,
    manager
  from query_3597103
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
    sd.contract_address as pool_address,
    sd.tokenId as token_id,
    sd.trancheId as tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(sd.trancheId + 1 as double)) as date) as tranche_expiry_date,
    cast(sd.amount as double) / 1e18 as amount,
    sd.user,
    sd.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeDeposited sd
  
  union all

  select
    'deposit extended' as flow_type,
    de.evt_block_time as block_time,
    de.contract_address as pool_address,
    de.tokenId as token_id,
    de.initialTrancheId as tranche_id,
    de.newTrancheId as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(de.newTrancheId + 1 as double)) as date) as tranche_expiry_date,
    cast(de.topUpAmount as double) / 1e18 as amount,
    de.user,
    de.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_DepositExtended de

  union all

  select
    'withdraw' as flow_type,
    w.evt_block_time as block_time,
    w.contract_address as pool_address,
    w.tokenId as token_id,
    w.tranche as tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(w.tranche + 1 as double)) as date) as tranche_expiry_date,
    -1 * cast((w.amountStakeWithdrawn) as double) / 1e18 as amount,
    w.user,
    w.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_Withdraw w
  where w.amountStakeWithdrawn > 0

  union all

  select
    'stake burn' as flow_type,
    eb.evt_block_time as block_time,
    eb.contract_address as pool_address,
    cast(null as uint256) as token_id, -- ???
    cast(null as uint256) as tranche_id, -- ???
    cast(null as uint256) as new_tranche_id,
    cast(null as date) as tranche_expiry_date, -- ???
    -1 * cast(eb.amount as double) / 1e18 as amount,
    cast(null as varbinary) as user,
    eb.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeBurned eb
),

staked_nxm_adjusted as (
  select
    flow_type,
    block_time,
    pool_address,
    token_id,
    coalesce(new_tranche_id, tranche_id) as tranche_id,
    tranche_expiry_date,
    if(tranche_expiry_date < current_date, false, true) as is_active,
    sum(amount) over (partition by pool_address, token_id, coalesce(new_tranche_id, tranche_id) order by block_time) as amount,
    user,
    tx_hash
  from staked_nxm_history
  where flow_type in ('deposit', 'deposit extended')

  union all

  select
    flow_type,
    block_time,
    pool_address,
    token_id,
    tranche_id,
    tranche_expiry_date,
    if(tranche_expiry_date < current_date, false, true) as is_active,
    amount,
    user,
    tx_hash
  from staked_nxm_history
  where flow_type not in ('deposit', 'deposit extended')
)

select
  sp.pool_id,
  t.flow_type,
  t.block_time,
  t.token_id,
  t.tranche_id,
  t.tranche_expiry_date,
  t.is_active,
  t.amount,
  t.tx_hash
from staking_pools sp
  left join staked_nxm_adjusted t on sp.pool_address = t.pool_address
--where sp.pool_id = 1
order by sp.pool_id, t.block_time
