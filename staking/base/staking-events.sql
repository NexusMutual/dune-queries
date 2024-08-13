with

staked_nxm_history as (
  select
    'deposit' as flow_type,
    sd.evt_block_time as block_time,
    sd.contract_address as pool_address,
    sd.tokenId as token_id,
    sd.trancheId as tranche_id,
    cast(null as uint256) as init_tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(sd.trancheId + 1 as double)) as date) as tranche_expiry_date,
    cast(sd.amount as double) / 1e18 as amount,
    cast(null as double) as topup_amount,
    sd.user,
    sd.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeDeposited sd
  
  union all

  select
    'deposit extended' as flow_type,
    de.evt_block_time as block_time,
    de.contract_address as pool_address,
    de.tokenId as token_id,
    cast(null as uint256) as tranche_id,
    de.initialTrancheId as init_tranche_id,
    de.newTrancheId as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(de.newTrancheId + 1 as double)) as date) as tranche_expiry_date,
    cast(null as double) as amount,
    cast(de.topUpAmount as double) / 1e18 as topup_amount,
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
    cast(null as uint256) as init_tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(from_unixtime(91.0 * 86400.0 * cast(w.tranche + 1 as double)) as date) as tranche_expiry_date,
    -1 * cast((w.amountStakeWithdrawn) as double) / 1e18 as amount,
    cast(null as double) as topup_amount,
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
    cast(null as uint256) as init_tranche_id,
    cast(null as uint256) as new_tranche_id,
    cast(null as date) as tranche_expiry_date, -- ???
    -1 * cast(eb.amount as double) / 1e18 as amount,
    cast(null as double) as topup_amount,
    cast(null as varbinary) as user,
    eb.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_StakeBurned eb
)

select
  flow_type,
  block_time,
  pool_address,
  token_id,
  tranche_id,
  init_tranche_id,
  new_tranche_id,
  tranche_expiry_date,
  if(tranche_expiry_date > current_date, true, false) as is_active,
  amount,
  topup_amount,
  user,
  tx_hash
from staked_nxm_history
