with

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
    date_trunc('month', block_date) as block_month,
    -1 * sum(nxm_cover_buy_burn) as nxm_cover_buy_burn,
    sum(nxm_rewards_mint) as nxm_rewards_mint
  from cover_buy_movements
  group by 1
),

stake_burn_for_claims as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    evt_block_date as block_date,
    amount / 1e18 as nxm_amount,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.stakingpool_evt_stakeburned
),

stake_burn_for_claims_agg as (
  select
    date_trunc('month', block_date) as block_month,
    -1 * sum(nxm_amount) as nxm_claim_burn
  from stake_burn_for_claims
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
    nxm_nxm_in as nxm_withdrawal,
    nxm_nxm_out as nxm_contribution
  from query_4841361 -- ramm volume
)

select
  ns.block_month,
  ns.nxm_supply_start,
  ns.nxm_supply_end,
  cbm.nxm_cover_buy_burn,
  cbm.nxm_rewards_mint,
  coalesce(sbc.nxm_claim_burn, 0) as nxm_claim_burn,
  coalesce(ar.nxm_assessor_rewards, 0) as nxm_assessor_rewards,
  coalesce(gr.nxm_gov_rewards, 0) as nxm_gov_rewards,
  cm.nxm_contribution,
  cm.nxm_withdrawal
from nxm_supply ns
  inner join cover_buy_movements_agg cbm on ns.block_month = cbm.block_month
  inner join capital_movement cm on ns.block_month = cm.block_month
  left join stake_burn_for_claims_agg sbc on ns.block_month = sbc.block_month
  left join assessor_rewards_agg ar on ns.block_month = ar.block_month
  left join gov_rewards_agg gr on ns.block_month = gr.block_month
order by 1 desc
