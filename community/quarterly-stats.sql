with

params as (
  select cast(case '{{quarter}}'
      when 'current quarter' then cast(date_trunc('quarter', current_date) as varchar)
      when 'last quarter' then cast(date_add('quarter', -1, date_trunc('quarter', current_date)) as varchar)
      else '{{quarter}}'
    end as timestamp) as period_date
),

cover_sales as (
  select
    sum(cover_start_usd) as cover_usd,
    sum(cover_start_eth) as cover_eth,
    sum(premium_nxm) as premium_nxm,
    sum(premium_usd) as premium_usd
  from query_3810247 -- full list of covers v2
  where date_trunc('quarter', cover_start_time) in (select period_date from params)
),

claims_paid as (
  select
    sum(eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount) as usd_claim_amount,
    sum(eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount) as eth_claim_amount
  --from query_3911051 -- claims paid base (fallback) query
  from nexusmutual_ethereum.claims_paid
  where date_trunc('quarter', coalesce(claim_payout_date, claim_date)) in (select period_date from params)
),

commissions as (
  select
    sum(usd_commission) as usd_commission,
    sum(eth_commission) as eth_commission
  from query_3926339 -- cover commission distribution
  where commission_destination = 'Community Fund'
    and date_trunc('quarter', cover_month) in (select period_date from params)
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

staking_rewards as (
  select
    sum(r.reward_total * p.avg_nxm_usd_price) as usd_rewards,
    sum(r.reward_total * p.avg_nxm_eth_price) as eth_rewards
  from query_4068272 r -- daily staking rewards base query
    cross join latest_prices p
  where date_trunc('quarter', r.block_date) in (select period_date from params)
)

select
  -- cover sales
  cs.cover_usd,
  cs.cover_eth,
  cs.premium_usd,
  cs.premium_nxm,
  -- claims paid
  cp.usd_claim_amount,
  cp.eth_claim_amount,
  -- DAO commissions
  c.usd_commission,
  c.eth_commission,
  -- staking rewards
  sr.usd_rewards,
  sr.eth_rewards
from cover_sales cs, claims_paid cp, commissions c, staking_rewards sr
