with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

covers as (
  select
    c.cover_id,
    cover_start_date,
    cover_end_date,
    c.premium_asset,
    c.premium * if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price) as usd_premium,
    c.premium * if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price) / p.avg_eth_usd_price as eth_premium
  --from query_3788367 c -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1 c
    inner join daily_avg_prices p on c.block_date = p.block_date
  union all
  select
    c.cover_id,
    cover_start_date,
    cover_end_date,
    c.premium_asset,
    c.premium_incl_commission * p.avg_nxm_usd_price as usd_premium,
    c.premium_incl_commission * p.avg_nxm_eth_price as eth_premium
  from query_4599092 c -- covers v2 - base root (fallback query)
    inner join daily_avg_prices p on c.block_date = p.block_date
  where c.is_migrated = false
),

premium_aggs as (
  select
    date_trunc('month', cover_start_date) as cover_month,
    sum(usd_premium) as usd_premium,
    sum(eth_premium) as eth_premium
  from covers
  group by 1
),

claims_paid as (
  select
    version,
    cover_id,
    claim_id,
    coalesce(claim_payout_date, claim_date) as claim_payout_date,
    --ETH
    eth_eth_claim_amount,
    eth_usd_claim_amount,
    --DAI
    dai_eth_claim_amount,
    dai_usd_claim_amount,
    --USDC
    usdc_eth_claim_amount,
    usdc_usd_claim_amount,
    --cbBTC
    cbbtc_eth_claim_amount,
    cbbtc_usd_claim_amount
  from query_3911051 -- claims paid base (fallback query)
  --from nexusmutual_ethereum.claims_paid
),

claims_paid_agg as (
  select
    date_trunc('month', claim_payout_date) as claim_payout_month,
    sum(eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount + cbbtc_usd_claim_amount) as usd_claim_paid,
    sum(eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount + cbbtc_eth_claim_amount) as eth_claim_paid
  from claims_paid
  group by 1
)

select
  p.cover_month,
  p.eth_premium,
  p.usd_premium,
  coalesce(cp.eth_claim_paid, 0) as eth_claim_paid,
  coalesce(cp.usd_claim_paid, 0) as usd_claim_paid,
  p.eth_premium - coalesce(cp.eth_claim_paid, 0) as eth_premiums_minus_claims,
  p.usd_premium - coalesce(cp.usd_claim_paid, 0) as usd_premiums_minus_claims
from premium_aggs p
  left join claims_paid_agg cp on p.cover_month = cp.claim_payout_month
order by 1 desc
