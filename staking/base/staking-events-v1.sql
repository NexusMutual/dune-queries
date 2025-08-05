/*
select sum(amount) / 1e18 from nexusmutual_ethereum.pooledstaking_evt_rewardadded
select sum(amount) / 1e18 from nexusmutual_ethereum.pooledstaking_evt_rewarded
select sum(amount) / 1e18 from nexusmutual_ethereum.pooledstaking_evt_burned
*/

with

staking_events as (
  select
    evt_block_time as block_time,
    evt_block_date as block_date,
    evt_block_number as block_number,
    null as product_address,
    'deposit' as event_type,
    staker,
    amount / 1e18 as amount,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.pooledstaking_evt_deposited
  union all
  select
    evt_block_time as block_time,
    evt_block_date as block_date,
    evt_block_number as block_number,
    null as product_address,
    'withdraw' as event_type,
    staker,
    -1 * (amount / 1e18) as amount,
    evt_index,
    evt_tx_hash
  from nexusmutual_ethereum.pooledstaking_evt_withdrawn
  union all
  select
    evt_block_time as block_time,
    evt_block_date as block_date,
    evt_block_number as block_number,
    contractAddress as product_address,
    'stake' as event_type,
    staker,
    amount / 1e18 as amount,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.pooledstaking_evt_staked
  union all
  select
    evt_block_time as block_time,
    evt_block_date as block_date,
    evt_block_number as block_number,
    contractAddress as product_address,
    'unstake' as event_type,
    staker,
    -1 * (amount / 1e18) as amount,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.pooledstaking_evt_unstaked
  union all
  select
    evt_block_time as block_time,
    evt_block_date as block_date,
    evt_block_number as block_number,
    null as product_address,
    'rewards' as event_type,
    staker,
    amount / 1e18 as amount,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.pooledstaking_evt_rewardwithdrawn
)

select
  block_time,
  block_date,
  block_number,
  event_type,
  product_address,
  staker,
  amount,
  tx_hash
from staking_events
