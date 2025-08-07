/*
nexusmutual_ethereum.tokenfunctions_call_pushstakerrewards
nexusmutual_ethereum.tokenfunctions_call_unlockstakerunlockabletokens
nexusmutual_ethereum.tokenfunctions_call_updatestakercommissions
*/

with

staking_events as (
  select
    s.call_block_time as block_time,
    s.call_block_date as block_date,
    s.call_block_number as block_number,
    s._scAddress as product_address,
    'stake' as event_type,
    l._of as staker,
    l._amount / 1e18 as amount,
    --l._validity as locked_validity,
    s.call_tx_hash as tx_hash
  from nexusmutual_ethereum.tokenfunctions_call_addstake s
    inner join nexusmutual_ethereum.tokencontroller_evt_locked l on s.call_block_time = l.evt_block_time and s.call_tx_hash = l.evt_tx_hash
  where s.call_success
  union all
  select
    s.call_block_time as block_time,
    s.call_block_date as block_date,
    s.call_block_number as block_number,
    null as product_address,
    'unstake' as event_type,
    l._of as staker,
    -1 * (l._amount / 1e18) as amount,
    s.call_tx_hash as tx_hash
  from nexusmutual_ethereum.tokenfunctions_call_unlockstakerunlockabletokens s
    inner join nexusmutual_ethereum.tokencontroller_evt_unlocked l on s.call_block_time = l.evt_block_time and s.call_tx_hash = l.evt_tx_hash
  where s.call_success
    and l._amount > 0
)

select
  block_time,
  block_date,
  block_number,
  product_address,
  event_type,
  staker,
  amount,
  tx_hash
from staking_events
