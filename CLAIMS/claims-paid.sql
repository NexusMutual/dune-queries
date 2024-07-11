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
  from query_3911051 -- claims paid base (fallback) query
)

select
  claim_date,
  sum(if('{{display_currency}}' = 'USD', eth_usd_claim_amount, eth_eth_claim_amount)) over (order by claim_date) as eth_claim_total,
  sum(if('{{display_currency}}' = 'USD', dai_usd_claim_amount, dai_eth_claim_amount)) over (order by claim_date) as dai_claim_total,
  sum(if('{{display_currency}}' = 'USD', usdc_usd_claim_amount, usdc_eth_claim_amount)) over (order by claim_date) as usdc_claim_total,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount,
    eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount
  )) over (order by claim_date) as claim_total
from claims_paid
where claim_date >= timestamp '{{Start Date}}'
  and claim_date < timestamp '{{End Date}}'
order by 1 desc
