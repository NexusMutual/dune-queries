with

claims_paid as (
  select
    version,
    cover_id,
    claim_id,
    claim_date,
    claim_payout_date,
    product_type,
    product_name,
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

reimbursements as (
  select
    block_date,
    product_name,
    eth_eth_reimbursement_amount,
    eth_usd_reimbursement_amount,
    dai_eth_reimbursement_amount,
    dai_usd_reimbursement_amount,
    usdc_eth_reimbursement_amount,
    usdc_usd_reimbursement_amount
  from query_4877015 -- claim reimbursements
)

select
  product_name,
  'claim paid' as flow_type,
  sum(if('{{display_currency}}' = 'USD', eth_usd_claim_amount, eth_eth_claim_amount)) as eth_claim_total,
  sum(if('{{display_currency}}' = 'USD', dai_usd_claim_amount, dai_eth_claim_amount)) as dai_claim_total,
  sum(if('{{display_currency}}' = 'USD', usdc_usd_claim_amount, usdc_eth_claim_amount)) as usdc_claim_total,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount,
    eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount
  )) as claim_total
from claims_paid
where claim_date >= timestamp '{{Start Date}}'
  and claim_date < timestamp '{{End Date}}'
group by 1
union all
select
  product_name,
  'reimbursement' as flow_type,
  -1 * sum(if('{{display_currency}}' = 'USD', eth_usd_reimbursement_amount, eth_eth_reimbursement_amount)) as eth_claim_total,
  -1 * sum(if('{{display_currency}}' = 'USD', dai_usd_reimbursement_amount, dai_eth_reimbursement_amount)) as dai_claim_total,
  -1 * sum(if('{{display_currency}}' = 'USD', usdc_usd_reimbursement_amount, usdc_eth_reimbursement_amount)) as usdc_claim_total,
  -1 * sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_reimbursement_amount + dai_usd_reimbursement_amount + usdc_usd_reimbursement_amount,
    eth_eth_reimbursement_amount + dai_eth_reimbursement_amount + usdc_eth_reimbursement_amount
  )) as claim_total
from reimbursements
group by 1
order by claim_total desc
