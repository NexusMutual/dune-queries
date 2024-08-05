with

claims_paid as (
  select
    version,
    cover_id,
    claim_id,
    claim_date,
    claim_payout_date,
    --ETH
    eth_eth_claim_amount,
    eth_usd_claim_amount,
    --DAI
    dai_eth_claim_amount,
    dai_usd_claim_amount,
    --USDC
    usdc_eth_claim_amount,
    usdc_usd_claim_amount
  --from query_3911051 -- claims paid base (fallback) query
  from nexusmutual_ethereum.claims_paid
),

claims_paid_agg as (
  select
    coalesce(claim_payout_date, claim_date) as claim_payout_date,
    sum(eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount) as eth_claim_amount,
    sum(eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount) as usd_claim_amount
  from claims_paid
  group by 1
),

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
    sum(tx_fee_eth) as eth_tx_fee_total,
    sum(tx_fee_usd) as usd_tx_fee_total
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
  -- claims paid
  coalesce(cp.eth_claim_amount, 0) as eth_claim_amount,
  coalesce(cp.usd_claim_amount, 0) as usd_claim_amount,
  -- tx gas fees
  coalesce(ctg.eth_tx_fee_total, 0) as eth_tx_fee_total,
  coalesce(ctg.usd_tx_fee_total, 0) as usd_tx_fee_total,
  coalesce(ctg.eth_tx_fee_total / ac.eth_premium, 0) as pct_eth_premium_tx_fee
--from query_3889661 ac -- BD active cover base
from nexusmutual_ethereum.covers_daily_agg ac
  left join cover_tx_gas_agg ctg on ac.block_date = ctg.block_date
  left join claims_paid_agg cp on ac.block_date = cp.claim_payout_date
order by 1 desc
