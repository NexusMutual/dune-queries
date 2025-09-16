select
  count(distinct row(version, claim_id)) as claims_paid_count,
  sum(eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount + cbbtc_usd_claim_amount) as claims_paid_total_usd
from query_5785588 -- claims paid - base root
--where version = 2
