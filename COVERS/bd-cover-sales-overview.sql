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
),

cover_tx_gas_agg as (
  select
    block_date,
    sum(tx_fee_eth) as total_tx_fee_eth,
    sum(tx_fee_usd) as total_tx_fee_usd
  from cover_tx_gas
  group by 1
)

select
  ac.block_date,
  -- cover sales
  ac.cover_sold,
  ac.eth_cover,
  ac.usd_cover,
  coalesce(ac.eth_cover / nullif(ac.cover_sold, 0), 0) as mean_eth_cover,
  ac.median_eth_cover,
  coalesce(ac.usd_cover / nullif(ac.cover_sold, 0), 0) as mean_usd_cover,
  ac.median_usd_cover,
  -- cover fees
  ac.eth_premium,
  ac.usd_premium,
  coalesce(ac.eth_premium / nullif(ac.cover_sold, 0), 0) as mean_eth_premium,
  ac.median_eth_premium,
  coalesce(ac.usd_premium / nullif(ac.cover_sold, 0), 0) as mean_usd_premium,
  ac.median_usd_premium,
  -- tx gas fees
  coalesce(ctg.total_tx_fee_eth, 0) as total_tx_fee_eth,
  coalesce(ctg.total_tx_fee_eth / ac.eth_premium, 0) as pct_premium_tx_fee_eth,
  coalesce(ctg.total_tx_fee_usd, 0) as total_tx_fee_usd,
  coalesce(ctg.total_tx_fee_usd / ac.usd_premium, 0) as pct_premium_tx_fee_usd
from query_3889661 ac -- BD active cover base
  left join cover_tx_gas_agg ctg on ac.block_date = ctg.block_date
order by 1 desc
