-- USDC Senior Tranche
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
  where owner in (select address from address_to_check)
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    contract_address,
    'withdraw' as event_type,
    owner as account,
    assets / 1e6 as usdc_amount,
    shares / 1e6 as sr_usdc_shares,
    evt_tx_hash as tx_hash
  from arcadia_v2_base.sr_usdc_evt_withdraw
  where owner in (select address from address_to_check)
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


-- lp contracts - 14 rows
-- deposits to sub-accounts
select
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_from,
  evt_tx_to,
  caller,
  owner,
  assets,
  shares,
  evt_tx_hash as tx_hash
from arcadia_v2_base.usdc_lp_evt_deposit
where evt_tx_from in (
  0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12,
  0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6,
  0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A,
  0xD5B00b396f4d11fe7d702551D74a95005E646323
)
order by 1

-- borrows from sub-accounts
select
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_from,
  evt_tx_to,
  account,
  contract_address as lending_pool_contract,
  amount,
  evt_tx_hash as tx_hash
from arcadia_v2_base.usdc_lp_evt_borrow
where evt_tx_from in (
  0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12,
  0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6,
  0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A,
  0xD5B00b396f4d11fe7d702551D74a95005E646323
)
order by 1

-- repayments to sub-accounts
select
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_from,
  evt_tx_to,
  account,
  contract_address as lending_pool_contract,
  amount,
  evt_tx_hash as tx_hash
from arcadia_v2_base.usdc_lp_evt_repay
where evt_tx_from in (
  0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12,
  0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6,
  0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A,
  0xD5B00b396f4d11fe7d702551D74a95005E646323
)
order by 1

-- withdrawals from sub-accounts
select
  evt_block_time as block_time,
  evt_block_number as block_number,
  evt_tx_from,
  evt_tx_to,
  contract_address as lending_pool_contract,
  owner,
  caller,
  receiver,
  assets,
  shares,
  evt_tx_hash as tx_hash
from arcadia_v2_base.usdc_lp_evt_withdraw
where evt_tx_from in (
  0x13f210c8baf5f5dbaff3e917e2e5a49e73bbaf12,
  0x97b418F3F1aBe6810Bed881Cfc298491f4b93Bf6,
  0x47F74Aaa5AFdF83087b60B530F27f44D94fa570A,
  0xD5B00b396f4d11fe7d702551D74a95005E646323
)
order by 1




WITH

usdc_borrows AS (
  SELECT
    0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 AS asset,
    amount,
    referrer
  FROM arcadia_v2_base.usdc_lp_evt_Borrow
)

SELECT
  cte.asset,
  TRY_CAST(SUM(cte.amount) as REAL) * p.price / POW(10, p.decimals) AS total_borrow,
  p.price
FROM usdc_borrows AS cte
JOIN prices.usd_latest AS p
  ON p.contract_address = cte.asset AND p.blockchain = 'base'
GROUP BY cte.asset, p.price, p.decimals
order by cte.asset
