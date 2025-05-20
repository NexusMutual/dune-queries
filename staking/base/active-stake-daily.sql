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
    asu.evt_block_number as block_number,
    asu.activeStake / 1e18 as active_stake,
    asu.stakeSharesSupply as stake_shares_supply,
    asu.evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingPool_evt_ActiveStakeUpdated asu
    inner join staking_pools sp on asu.contract_address = sp.pool_address
),

daily_snapshots as (
  select
    block_date,
    pool_id,
    pool_address,
    active_stake,
    tx_hash,
    row_number() over (partition by pool_id, block_date order by block_time desc) as pool_date_rn
  from active_stake_updates
),

daily_snapshots_with_next as (
  select
    *,
    lead(block_date) over (partition by pool_id order by block_date) as next_update_date
  from (
    -- regular incremental load
    select *, cast(null as bigint) as rn
    from daily_snapshots
    where pool_date_rn = 1
    /*
    -- find last snapshot for each pool pre incremental load window
    union all
    select *
    from (
      select *, row_number() over (partition by pool_id order by block_date desc) as rn
      from daily_snapshots
      where not incremental_predicate
    )
    where rn = 1
    */
  ) t
),

pool_start_dates as (
  select
    pool_id,
    pool_address,
    min(block_date) as block_date_start
  from daily_snapshots_with_next
  group by 1, 2
),

daily_sequence as (
  select
    s.pool_id,
    s.pool_address,
    d.timestamp as block_date
  from utils.days d
    inner join pool_start_dates s on d.timestamp >= s.block_date_start
  where d.timestamp <= current_date
),

forward_fill as (
  select
    s.pool_id,
    s.pool_address,
    s.block_date,
    dc.active_stake,
    dc.tx_hash
  from daily_sequence s
    left join daily_snapshots_with_next dc
      on s.pool_id = dc.pool_id
      and s.block_date >= dc.block_date
      and (s.block_date < dc.next_update_date or dc.next_update_date is null)
)

select
  block_date,
  pool_id,
  pool_address,
  active_stake,
  tx_hash
from forward_fill
--where pool_id = 22
--order by 1, 2, 3 desc
