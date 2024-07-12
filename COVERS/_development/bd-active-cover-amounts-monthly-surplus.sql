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
),

claims_paid_agg as (
  select
    coalesce(claim_payout_date, claim_date) as claim_payout_date,
    sum(eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount) as eth_claim_amount,
    sum(eth_eth_claim_amount) as eth_eth_claim_amount,
    sum(dai_eth_claim_amount) as dai_eth_claim_amount,
    sum(usdc_eth_claim_amount) as usdc_eth_claim_amount,
    sum(eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount) as usd_claim_amount,
    sum(eth_usd_claim_amount) as eth_usd_claim_amount,
    sum(dai_usd_claim_amount) as dai_usd_claim_amount,
    sum(usdc_usd_claim_amount) as usdc_usd_claim_amount
  from claims_paid
  group by 1
)

select
  date_trunc('month', ac.block_date) as block_month,
  -- sum of the number of covers sold in any given month:
  sum(ac.cover_sold) as cover_sold,
  -- Monthly Active Cover = average Active Cover Amount:
  avg(ac.eth_active_cover) as avg_eth_active_cover,
  avg(ac.eth_eth_active_cover) as avg_eth_eth_cover,
  avg(ac.dai_eth_active_cover) as avg_dai_eth_cover,
  avg(ac.usdc_eth_active_cover) as avg_usdc_eth_cover,
  avg(ac.usd_active_cover) as avg_usd_active_cover,
  avg(ac.eth_usd_active_cover) as avg_eth_usd_cover,
  avg(ac.dai_usd_active_cover) as avg_dai_usd_cover,
  avg(ac.usdc_usd_active_cover) as avg_usdc_usd_cover,
  -- Monthly Cover Amount = sum of the Cover Amount for all covers sold in any given month
  sum(ac.eth_cover) as eth_cover,
  sum(ac.eth_eth_cover) as eth_eth_cover,
  sum(ac.dai_eth_cover) as dai_eth_cover,
  sum(ac.usdc_eth_cover) as usdc_eth_cover,
  sum(ac.usd_cover) as usd_cover,
  sum(ac.eth_usd_cover) as eth_usd_cover,
  sum(ac.dai_usd_cover) as dai_usd_cover,
  sum(ac.usdc_usd_cover) as usdc_usd_cover,
  -- Monthly Premium = sum of the premiums paid in any given month
  sum(ac.eth_premium - coalesce(cp.eth_claim_amount, 0)) as eth_premium,
  sum(ac.eth_eth_premium - coalesce(cp.eth_eth_claim_amount, 0)) as eth_eth_premium,
  sum(ac.dai_eth_premium - coalesce(cp.dai_eth_claim_amount, 0)) as dai_eth_premium,
  sum(ac.nxm_eth_premium - coalesce(cp.nxm_eth_claim_amount, 0)) as nxm_eth_premium,
  sum(ac.usd_premium - coalesce(cp.usd_claim_amount, 0)) as usd_premium,
  sum(ac.eth_usd_premium - coalesce(cp.eth_usd_claim_amount, 0)) as eth_usd_premium,
  sum(ac.dai_usd_premium - coalesce(cp.dai_usd_claim_amount, 0)) as dai_usd_premium,
  sum(ac.nxm_usd_premium - coalesce(cp.nxm_usd_claim_amount, 0)) as nxm_usd_premium
from query_3889661 ac -- BD active cover base
  left join claims_paid_agg cp on ac.block_date = cp.claim_payout_date
where ac.block_date >= now() - interval '3' year
group by 1
order by 1 desc
