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

membership as (
  -- v1
  select
    1 as version,
    date_trunc('day', call_block_time) as block_date,
    cardinality(userArray) as member_count
  from nexusmutual_ethereum.MemberRoles_call_addMembersBeforeLaunch
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  union all
  select
    1 as version,
    date_trunc('day', call_block_time) as block_date,
    count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_kycVerdict
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
    and verdict
  group by 2
  union all
  -- v2
  select
    2 as version,
    date_trunc('day', call_block_time) as block_date,
    count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_join
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  group by 2
  union all
  select
    2 as version,
    date_trunc('day', call_block_time) as block_date,
    -1 * count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_withdrawMembership
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  group by 2
),

membership_agg as (
  select
    date_trunc('month', m.block_date) as block_month,
    sum(m.member_count) as member_count,
    sum(m.member_count * 0.0020) as eth_member_fee,
    sum(m.member_count * 0.0020 * p.avg_eth_usd_price) as usd_member_fee
  from membership m
    inner join daily_avg_prices p on m.block_date = p.block_date
  group by 1
),

claims_paid as (
  select
    version,
    cover_id,
    claim_id,
    coalesce(claim_payout_date, claim_date) as claim_payout_date,
    eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount + cbbtc_usd_claim_amount as usd_claim_paid,
    eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount + cbbtc_eth_claim_amount as eth_claim_paid
  from query_5785588 -- claims paid - base root
),

claims_paid_agg as (
  select
    date_trunc('month', claim_payout_date) as claim_payout_month,
    -1 * sum(usd_claim_paid) as usd_claim_paid,
    -1 * sum(eth_claim_paid) as eth_claim_paid
  from claims_paid
  group by 1
),

claim_reimbursements as (
  select
    block_date,
    eth_usd_reimbursement_amount + dai_usd_reimbursement_amount + usdc_usd_reimbursement_amount as usd_reimbursement,
    eth_eth_reimbursement_amount + dai_eth_reimbursement_amount + usdc_eth_reimbursement_amount as eth_reimbursement
  from query_4877015 -- claim reimbursements
),

claim_reimbursements_agg as (
  select
    date_trunc('month', block_date) as block_month,
    sum(usd_reimbursement) as usd_reimbursement,
    sum(eth_reimbursement) as eth_reimbursement
  from claim_reimbursements
  group by 1
)

select
  p.cover_month,
  p.eth_premium,
  p.usd_premium,
  coalesce(m.eth_member_fee, 0) as eth_member_fee,
  coalesce(m.usd_member_fee, 0) as usd_member_fee,
  coalesce(cp.eth_claim_paid, 0) as eth_claim_paid,
  coalesce(cp.usd_claim_paid, 0) as usd_claim_paid,
  coalesce(cr.eth_reimbursement, 0) as eth_reimbursement,
  coalesce(cr.usd_reimbursement, 0) as usd_reimbursement
from premium_aggs p
  left join membership_agg m on p.cover_month = m.block_month
  left join claims_paid_agg cp on p.cover_month = cp.claim_payout_month
  left join claim_reimbursements_agg cr on p.cover_month = cr.block_month
order by 1 desc
