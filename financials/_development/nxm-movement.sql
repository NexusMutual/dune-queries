with

daily_avg_prices as (
  select
    block_date,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

cover_buy_movements as (
  select
    c.block_date,
    c.cover_id,
    bf.amount / 1e18 as nxm_cover_buy_burn,
    bf.amount / 1e18 * 0.5 as nxm_rewards_mint,
    c.tx_hash
  from query_4599092 c -- covers v2 - base root (fallback query)
    inner join nexusmutual_ethereum.tokencontroller_call_burnfrom bf
      on c.block_time = bf.call_block_time
      and c.block_number = bf.call_block_number
      and c.tx_hash = bf.call_tx_hash
  where c.is_migrated = false
    and c.premium_asset = 'NXM'
),

cover_buy_movements_agg as (
  select
    date_trunc('month', block_date) as block_month,
    -1 * sum(nxm_cover_buy_burn) as nxm_cover_buy_burn,
    sum(nxm_rewards_mint) as nxm_rewards_mint
  from cover_buy_movements
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
  --from query_3911051 -- claims paid base (fallback query)
  from nexusmutual_ethereum.claims_paid
),

claims_paid_agg as (
  select
    date_trunc('month', claim_payout_date) as claim_payout_month,
    -1 * sum(usd_claim_paid) as usd_claim_paid,
    -1 * sum(eth_claim_paid) as eth_claim_paid
  from claims_paid
  group by 1
),

assessor_rewards as (
  select distinct
    call_block_time as block_time,
    call_block_number as block_number,
    date_trunc('day', call_block_time) as block_date,
    _0 as claim_id,
    output_totalRewardInNXM / 1e18 as nxm_assessor_rewards,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.Assessment_call_assessments
  where call_success
),

assessor_rewards_agg as (
  select
    date_trunc('month', block_date) as block_month,
    sum(nxm_assessor_rewards) as nxm_assessor_rewards
  from assessor_rewards
  group by 1
),

gov_rewards as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    date_trunc('day', evt_block_time) as block_date,
    gbtReward / 1e18 as nxm_gov_rewards,
    evt_tx_hash
  from nexusmutual_ethereum.governance_evt_rewardclaimed
),

gov_rewards_agg as (
  select
    date_trunc('month', block_date) as block_month,
    sum(nxm_gov_rewards) as nxm_gov_rewards
  from gov_rewards
  group by 1
),

capital_movement as (
  select
    block_month,
    eth_nxm_in,
    usd_nxm_in,
    eth_nxm_out,
    usd_nxm_out
  from query_4841361 -- ramm volume
)

select
  cbm.block_month,
  cbm.nxm_cover_buy_burn,
  cbm.nxm_rewards_mint,
  cp.eth_claim_paid,
  cp.usd_claim_paid,
  ar.nxm_assessor_rewards,
  gr.nxm_gov_rewards,
  cm.eth_nxm_in,
  cm.usd_nxm_in,
  cm.eth_nxm_out,
  cm.usd_nxm_out
from cover_buy_movements_agg cbm
  inner join capital_movement cm on cbm.block_month = cm.block_month
  left join claims_paid_agg cp on cbm.block_month = cp.claim_payout_month
  left join assessor_rewards_agg ar on cbm.block_month = ar.block_month
  left join gov_rewards_agg gr on cbm.block_month = gr.block_month
order by 1 desc
