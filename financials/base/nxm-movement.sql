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

staking_rewards_mint as (
  select
    block_date,
    cover_id,
    reward_amount_expected_total,
    tx_hash
  from query_4067736 -- staking rewards base
  --from nexusmutual_ethereum.staking_rewards
),

staking_rewards_mint_agg as (
  select
    date_trunc('month', block_date) as block_month,
    sum(reward_amount_expected_total) as nxm_reward_mint
  from staking_rewards_mint
  group by 1
),

covers as (
  select distinct
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    cover_id,
    tx_hash
  from query_4599092 -- covers v2 - base root (fallback query)
  where is_migrated = false
    and premium_asset = 'NXM'
),

cover_buy_burn as (
  select
    c.block_date,
    c.cover_id,
    bf.amount / 1e18 as nxm_cover_buy_burn,
    c.tx_hash
  from nexusmutual_ethereum.tokencontroller_call_burnfrom bf
    inner join covers c on bf.call_block_time = c.block_time and bf.call_block_number = c.block_number and bf.call_tx_hash = c.tx_hash
),

cover_buy_burn_agg as (
  select
    date_trunc('month', block_date) as block_month,
    -1 * sum(nxm_cover_buy_burn) as nxm_cover_buy_burn
  from cover_buy_burn
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
    pa.proposalId,
    t.block_date,
    t.amount,
    t.tx_hash
  from nexusmutual_ethereum.governance_evt_proposalaccepted pa
    inner join tokens_ethereum.transfers t on pa.evt_block_time = t.block_time and pa.evt_block_number = t.block_number and pa.evt_tx_hash = t.tx_hash
  where t.symbol = 'NXM'
    and t."from" = 0x0000000000000000000000000000000000000000
    and t.amount > 0
),

gov_rewards_agg as (
  select
    date_trunc('month', block_date) as block_month,
    sum(amount) as nxm_gov_rewards
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
  srm.nxm_reward_mint,
  cbm.nxm_cover_buy_burn,
  coalesce(sbc.nxm_claim_burn, 0) as nxm_claim_burn,
  coalesce(ar.nxm_assessor_rewards, 0) as nxm_assessor_rewards,
  coalesce(gr.nxm_gov_rewards, 0) as nxm_gov_rewards,
  cm.nxm_contribution,
  cm.nxm_withdrawal
from nxm_supply ns
  inner join staking_rewards_mint_agg srm on ns.block_month = srm.block_month
  inner join cover_buy_burn_agg cbm on ns.block_month = cbm.block_month
  inner join capital_movement cm on ns.block_month = cm.block_month
  left join stake_burn_for_claims_agg sbc on ns.block_month = sbc.block_month
  left join assessor_rewards_agg ar on ns.block_month = ar.block_month
  left join gov_rewards_agg gr on ns.block_month = gr.block_month
order by 1 desc
