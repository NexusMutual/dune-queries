with

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
  --from query_3911051 -- claims paid base (fallback query)
  from nexusmutual_ethereum.claims_paid
)

select
  date_trunc('month', claim_payout_date) as claim_payout_month,
  sum(eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount + cbbtc_usd_claim_amount) as usd_claim_paid,
  sum(eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount + cbbtc_eth_claim_amount) as eth_claim_paid
from claims_paid
group by 1
order by 1 desc
