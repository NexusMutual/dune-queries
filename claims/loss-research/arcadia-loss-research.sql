-- Arcadia - user assets in & out protocol movements (USDC Senior Tranche)
with

address_to_check (address) as (
  values
  (0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12),
  (0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6),
  (0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A),
  (0xD5B00b396f4d11fe7d702551D74a95005E646323)
),

asset_movements as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    contract_address,
    'deposit' as event_type,
    owner as account,
    assets / 1e6 as usdc_amount,
    shares / 1e6 as sr_usdc_shares,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.sr_usdc_evt_deposit
  where evt_tx_from in (select address from address_to_check)
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    contract_address,
    'withdraw' as event_type,
    owner as account,
    -1 * (assets / 1e6) as usdc_amount,
    -1 * (shares / 1e6) as sr_usdc_shares,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.sr_usdc_evt_withdraw
  where evt_tx_from in (select address from address_to_check)
)

select
  account,
  block_time,
  block_number,
  event_type,
  usdc_amount,
  sr_usdc_shares,
  sum(usdc_amount) over (partition by account order by block_time) as usdc_balance,
  sum(sr_usdc_shares) over (partition by account order by block_time) as sr_usdc_balance,
  tx_hash
from asset_movements
order by 1, 2



-- Arcadia - USDC lending pool movements
with

address_to_check (address) as (
  values
  (0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12),
  (0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6),
  (0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A),
  (0xD5B00b396f4d11fe7d702551D74a95005E646323)
),

asset_movements as (
  /*select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'deposit' as event_type,
    evt_tx_from as account,
    owner as sub_account,
    assets / 1e6 as usdc_amount,
    shares / 1e6 as usdc_lp_shares,
    evt_index,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.usdc_lp_evt_deposit
  where evt_tx_from in (select address from address_to_check)
  union all*/
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'borrow' as event_type,
    evt_tx_from as account,
    account as sub_account,
    -1 * (amount / 1e6) as usdc_amount,
    --null as usdc_lp_shares,
    evt_index,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.usdc_lp_evt_borrow
  where evt_tx_from in (select address from address_to_check)
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'repay' as event_type,
    evt_tx_from as account,
    account as sub_account,
    amount / 1e6 as usdc_amount,
    --null as usdc_lp_shares,
    evt_index,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.usdc_lp_evt_repay
  where evt_tx_from in (select address from address_to_check)
  /*union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'withdraw' as event_type,
    evt_tx_from as account,
    owner as sub_account,
    -1 * (assets / 1e6) as usdc_amount,
    -1 * (shares / 1e6) as usdc_lp_shares,
    evt_index,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.usdc_lp_evt_withdraw
  where evt_tx_from in (select address from address_to_check)*/
)

select
  account,
  sub_account,
  block_time,
  block_number,
  evt_index,
  event_type,
  usdc_amount,
  --usdc_lp_shares,
  sum(usdc_amount) over (partition by account, sub_account order by block_time, tx_hash, evt_index) as usdc_sub_account_balance,
  --sum(usdc_lp_shares) over (partition by account, sub_account order by block_time, tx_hash, evt_index) as usdc_lp_shares_sub_account_balance,
  sum(usdc_amount) over (partition by account order by block_time, tx_hash, evt_index) as usdc_account_balance,
  --sum(usdc_lp_shares) over (partition by account order by block_time, tx_hash, evt_index) as usdc_lp_shares_account_balance,
  evt_index,
  tx_hash
from asset_movements
order by 1, 2, 3, 5



-- Arcadia - USDC user holdings research
with

address_to_check (address) as (
  values
  (0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12),
  (0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6),
  (0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A),
  (0xD5B00b396f4d11fe7d702551D74a95005E646323)
),

asset_movements as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'deposit' as event_type,
    owner as account,
    null as sub_account,
    assets / 1e6 as usdc_amount,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.sr_usdc_evt_deposit
  where evt_tx_from in (select address from address_to_check)
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'withdraw' as event_type,
    owner as account,
    null as sub_account,
    -1 * (assets / 1e6) as usdc_amount,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.sr_usdc_evt_withdraw
  where evt_tx_from in (select address from address_to_check)
  union all
    select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'borrow' as event_type,
    evt_tx_from as account,
    account as sub_account,
    -1 * (amount / 1e6) as usdc_amount,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.usdc_lp_evt_borrow
  where evt_tx_from in (select address from address_to_check)
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'repay' as event_type,
    evt_tx_from as account,
    account as sub_account,
    amount / 1e6 as usdc_amount,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.usdc_lp_evt_repay
  where evt_tx_from in (select address from address_to_check)
)

select
  account,
  sub_account,
  block_time,
  block_number,
  event_type,
  usdc_amount,
  sum(usdc_amount) over (partition by account, sub_account order by block_time, tx_hash, evt_index) as usdc_sub_account_balance,
  sum(usdc_amount) over (partition by account order by block_time, tx_hash, evt_index) as usdc_account_balance,
  tx_hash
from asset_movements
order by 1, 3
