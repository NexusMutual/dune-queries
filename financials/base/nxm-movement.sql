with

prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

nxm_supply as (
  select
    date_trunc('month', block_date) as block_month,
    lag(total_nxm, 1) over (order by block_date) as nxm_supply_start,
    total_nxm as nxm_supply_end
  from (  
    select
      block_date,
      total_nxm,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from query_4514857 -- NXM supply base
  ) t
  where rn = 1
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
    date_trunc('month', cbm.block_date) as block_month,
    -1 * sum(cbm.nxm_cover_buy_burn * p.avg_nxm_eth_price) as eth_nxm_cover_buy_burn,
    -1 * sum(cbm.nxm_cover_buy_burn * p.avg_nxm_usd_price) as usd_nxm_cover_buy_burn,
    sum(cbm.nxm_rewards_mint * p.avg_nxm_eth_price) as eth_nxm_rewards_mint,
    sum(cbm.nxm_rewards_mint * p.avg_nxm_usd_price) as usd_nxm_rewards_mint
  from cover_buy_movements cbm
    inner join prices p on cbm.block_date = p.block_date
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
    date_trunc('month', ar.block_date) as block_month,
    sum(ar.nxm_assessor_rewards * p.avg_nxm_eth_price) as eth_nxm_assessor_rewards,
    sum(ar.nxm_assessor_rewards * p.avg_nxm_usd_price) as usd_nxm_assessor_rewards
  from assessor_rewards ar
    inner join prices p on ar.block_date = p.block_date
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
    date_trunc('month', gr.block_date) as block_month,
    sum(gr.nxm_gov_rewards * p.avg_nxm_eth_price) as eth_nxm_gov_rewards,
    sum(gr.nxm_gov_rewards * p.avg_nxm_usd_price) as usd_nxm_gov_rewards
  from gov_rewards gr
    inner join prices p on gr.block_date = p.block_date
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
),

prices_start_end as (
  select
    date_trunc('month', block_date) as block_month,
    -- start: last of the previous month
    lag(avg_eth_usd_price, 1) over (order by block_date) as eth_usd_price_start,
    lag(avg_nxm_eth_price, 1) over (order by block_date) as nxm_eth_price_start,
    lag(avg_nxm_usd_price, 1) over (order by block_date) as nxm_usd_price_start,
    -- end: last of the month
    avg_eth_usd_price as eth_usd_price_end,
    avg_nxm_eth_price as nxm_eth_price_end,
    avg_nxm_usd_price as nxm_usd_price_end
  from (
    select
      block_date,
      avg_eth_usd_price,
      avg_nxm_eth_price,
      avg_nxm_usd_price,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from prices
  ) t
  where rn = 1
)

select
  ns.block_month,
  -- prices
  p.eth_usd_price_start,
  p.nxm_eth_price_start,
  p.nxm_usd_price_start,
  p.eth_usd_price_end,
  p.nxm_eth_price_end,
  p.nxm_usd_price_end,
  -- ETH
  ns.nxm_supply_start * p.nxm_eth_price_start as eth_nxm_supply_start,
  ns.nxm_supply_end * p.nxm_eth_price_end as eth_nxm_supply_end,
  cbm.eth_nxm_cover_buy_burn,
  cbm.eth_nxm_rewards_mint,
  coalesce(cp.eth_claim_paid, 0) as eth_claim_paid,
  coalesce(ar.eth_nxm_assessor_rewards, 0) as eth_nxm_assessor_rewards,
  coalesce(gr.eth_nxm_gov_rewards, 0) as eth_nxm_gov_rewards,
  cm.eth_nxm_in,
  cm.eth_nxm_out,
  -- USD
  cbm.usd_nxm_cover_buy_burn,
  cbm.usd_nxm_rewards_mint,
  coalesce(cp.usd_claim_paid, 0) as usd_claim_paid,
  coalesce(ar.usd_nxm_assessor_rewards, 0) as usd_nxm_assessor_rewards,
  coalesce(gr.usd_nxm_gov_rewards, 0) as usd_nxm_gov_rewards,
  cm.usd_nxm_in,
  cm.usd_nxm_out
from nxm_supply ns
  inner join prices_start_end p on ns.block_month = p.block_month
  inner join cover_buy_movements_agg cbm on ns.block_month = cbm.block_month
  inner join capital_movement cm on ns.block_month = cm.block_month
  left join claims_paid_agg cp on ns.block_month = cp.claim_payout_month
  left join assessor_rewards_agg ar on ns.block_month = ar.block_month
  left join gov_rewards_agg gr on ns.block_month = gr.block_month
order by 1 desc
