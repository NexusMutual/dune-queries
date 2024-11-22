with

cover_tx_gas as (
  select
    c.block_date,
    c.cover_id,
    t.tx_fee_native as tx_fee_eth,
    t.tx_fee_usd,
    t.gas_price_gwei,
    t.gas_price_usd,
    t.gas_used,
    t.transaction_type,
    t.tx_hash
  from gas_ethereum.fees t
    --inner join query_3788367 c -- covers v1 base (fallback) query
    inner join nexusmutual_ethereum.covers_v1 c
      on t.block_number = c.block_number
      and t.block_time = c.block_time
      and t.tx_hash = c.tx_hash
  where t.block_time >= timestamp '2019-07-12'
  union all
  select
    c.block_date,
    c.cover_id,
    t.tx_fee_native as tx_fee_eth,
    t.tx_fee_usd,
    t.gas_price_gwei,
    t.gas_price_usd,
    t.gas_used,
    t.transaction_type,
    t.tx_hash
  from gas_ethereum.fees t
    --inner join query_3788370 c -- covers v2 base (fallback) query
    inner join nexusmutual_ethereum.covers_v2 c
      on t.block_number = c.block_number
      and t.block_time = c.block_time
      and t.tx_hash = c.tx_hash
  where t.block_time >= timestamp '2023-03-16'
)

select
  block_date,
  sum(tx_fee_eth) as total_tx_fee_eth,
  sum(tx_fee_usd) as total_tx_fee_usd
from cover_tx_gas
group by 1
