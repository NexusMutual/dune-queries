with

address_to_check (address) as (
  values
  (0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12),
  (0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6),
  (0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A),
  (0xD5B00b396f4d11fe7d702551D74a95005E646323)
),

deposits as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    'deposit' as flow_type,
    call_tx_from as account,
    call_tx_to as sub_account,
    assetAddresses as arr_asset_addresses,
    assetAmounts as arr_asset_amounts,
    call_tx_hash as tx_hash
  from arcadia_v2_base.accountv1_call_deposit
  where call_tx_from in (select address from address_to_check)
    and call_success
),

withdrawals as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    'withdraw' as flow_type,
    call_tx_from as account,
    call_tx_to as sub_account,
    assetAddresses as arr_asset_addresses,
    assetAmounts as arr_asset_amounts,
    call_tx_hash as tx_hash
  from arcadia_v2_base.accountv1_call_withdraw
  where call_tx_from in (select address from address_to_check)
    and call_success
),

movements as (
  select * from deposits
  union all
  select * from withdrawals
)

select
  m.block_time,
  m.block_number,
  m.flow_type,
  m.account,
  m.sub_account,
  a.asset_address,
  a.asset_amount,
  t.symbol,
  a.asset_amount / power(t.decimals, 10) as amount,
  m.tx_hash
from movements m
  cross join unnest (m.arr_asset_addresses, m.arr_asset_amounts) as a(asset_address, asset_amount)
  left join tokens.erc20 t on a.asset_address = t.contract_address and t.blockchain = 'base'
order by m.account, m.block_time
