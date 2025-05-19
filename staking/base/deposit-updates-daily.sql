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

deposit_updates as (
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

updates_combined as (
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
    asu.active_stake * du.stake_shares / du.stake_shares_supply as token_stake,
    cast(from_unixtime(91.0 * 86400.0 * cast(du.tranche_id + 1 as double)) as date) as tranche_expiry_date,
    du.tx_hash
  from deposit_updates du
    inner join active_stake_updates asu
      on du.pool_id = asu.pool_id
      and du.block_time = asu.block_time
      and du.tx_hash = asu.tx_hash
      and du.evt_index < asu.evt_index
      and asu.rn = 1
),

daily_snapshots as (
  select
    pool_id,
    pool_address,
    block_date,
    tranche_id,
    token_id,
    max_by(active_stake, block_time) as active_stake,
    max_by(stake_shares, block_time) as stake_shares,
    max_by(stake_shares_supply, block_time) as stake_shares_supply,
    max_by(token_stake, block_time) as token_stake,
    max_by(tranche_expiry_date, block_time) as tranche_expiry_date,
    max_by(tx_hash, block_time) as tx_hash
  from updates_combined
  group by 1, 2, 3, 4, 5
),

daily_snapshots_with_next as (
  select
    *,
    lead(block_date) over (partition by pool_id, tranche_id, token_id order by block_date) as next_update_date
  from (
    select * from daily_snapshots
    /*
    -- prep for incremental load
    union all
    select
      pool_id,
      pool_address,
      tranche_id,
      token_id,
      max(block_date) as block_date,
      max_by(active_stake, block_time) as active_stake,
      max_by(stake_shares_supply, block_time) as stake_shares_supply,
      max_by(token_stake, block_time) as token_stake,
      max_by(tranche_expiry_date, block_time) as tranche_expiry_date,
      max_by(tx_hash, block_time) as tx_hash
    from daily_snapshots
    group by 1, 2, 3, 4
    */
  ) t
),

pool_start_dates as (
  select
    pool_id,
    pool_address,
    tranche_id,
    token_id,
    min(block_date) as block_date_start
  from daily_snapshots_with_next
  group by 1, 2, 3, 4
),

daily_sequence as (
  select
    s.pool_id,
    s.pool_address,
    s.tranche_id,
    s.token_id,
    d.timestamp as block_date
  from utils.days d
    inner join pool_start_dates s on d.timestamp >= s.block_date_start
  where d.timestamp <= current_date
),

forward_fill as (
  select
    s.pool_id,
    s.pool_address,
    s.tranche_id,
    s.token_id,
    s.block_date,
    dc.active_stake,
    dc.stake_shares,
    dc.stake_shares_supply,
    dc.token_stake,
    dc.tranche_expiry_date,
    dc.tx_hash
  from daily_sequence s
    left join daily_snapshots_with_next dc
      on s.pool_id = dc.pool_id
      and s.tranche_id = dc.tranche_id
      and s.token_id = dc.token_id
      and s.block_date >= dc.block_date
      and (s.block_date < dc.next_update_date or dc.next_update_date is null)
)

select
  pool_id,
  pool_address,
  tranche_id,
  token_id,
  block_date,
  active_stake,
  stake_shares,
  stake_shares_supply,
  token_stake,
  tranche_expiry_date,
  tx_hash
from forward_fill
--order by pool_id, tranche_id, token_id, block_time
