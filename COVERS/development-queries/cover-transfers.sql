with

covers as (
  select distinct
    block_time,
    block_number,
    cover_id,
    cover_start_time,
    cover_end_time,
    --staking_pool,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    premium_asset,
    --premium,
    cover_owner,
    tx_hash
  from query_3678332 -- covers v2
),

cover_transfers as (
  select
    c.cover_id,
    c.cover_owner,
    c.cover_asset,
    c.sum_assured,
    c.premium_asset,
    t.block_time,
    t.block_number,
    t.tx_from, -- cover_owner
    t.tx_to,
    t."from" as transfer_from,
    t.to as transfer_to,
    t.symbol,
    t.amount_raw,
    t.amount,
    t.price_usd,
    t.amount_usd,
    t.evt_index,
    t.tx_hash
  from tokens_ethereum.transfers t
    inner join covers c on t.tx_hash = c.tx_hash and t.block_number = c.block_number
  where t.block_time >= timestamp '2023-03-09'
),

cover_totals as (
select
  cover_id,
  cover_owner,
  premium_asset,
  block_time,
  premium_asset,
  sum(amount) as amount,
  sum(amount_usd) as amount_usd,
  arbitrary(tx_hash) as tx_hash
from cover_transfers
--where cover_id in (220, 223, 735, 736)
where 1 = 1
  and premium_asset = symbol
  and cover_owner = transfer_from
group by 1,2,3,4,5
--order by 1 desc
)

--/*
select c.cover_id, c.cover_owner, c.premium_asset, c.tx_hash
from covers c
  left join cover_totals ct on c.cover_id = ct.cover_id
where ct.cover_id is null
order by 1
--*/

/*
select
  cover_id,
  cover_owner,
  --cover_asset,
  --sum_assured,
  premium_asset,
  block_time,
  symbol,
  amount,
  amount_usd,
  --tx_from,
  --tx_to,
  transfer_from,
  transfer_to,
  evt_index,
  tx_hash
from cover_transfers
where cover_id = 521
--where cover_id in (220, 223, 735, 736)
  --and premium_asset = symbol
  --and cover_owner = transfer_from
order by cover_id, evt_index, symbol
--*/


/*
select
  cover_id,
  tx_to,
  --sum(case when tx_from in (0x0000000000000000000000000000000000000000, cover_owner) and symbol in ('NXM', 'wNXM') then amount end) as nxm_amount,
  sum(
    case
      when tx_from = cover_owner and transfer_to = 0x5407381b6c251cFd498ccD4A1d877739CB7960B8 and symbol in ('NXM', 'wNXM')
      then amount
    end
  ) as cover_owner_to_token_controller_nxm_amount,
  sum(
    case
      when tx_from = cover_owner and transfer_to = 0x0000000000000000000000000000000000000000 and symbol in ('NXM', 'wNXM')
      then amount
    end
  ) as cover_owner_to_burn_nxm_amount,
  sum(
    case
      when tx_from = 0x0000000000000000000000000000000000000000 and transfer_to = 0x5407381b6c251cFd498ccD4A1d877739CB7960B8 and symbol in ('NXM', 'wNXM')
      then amount
    end
  ) as burn_to_token_controller_nxm_amount,
  --sum(amount) as amount,
  --sum(amount_usd) as amount_usd
  tx_hash
from cover_transfers
--where symbol in ('WETH', 'wNXM', 'USDC')
where cover_id in (220, 223, 735, 736)
group by 1,2,tx_hash
order by 1 desc
*/
