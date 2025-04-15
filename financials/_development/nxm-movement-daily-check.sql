with

nxm_supply as (
  select
    block_date,
    nxm_mint,
    nxm_burn,
    total_nxm as nxm_supply
  from query_4514857 -- NXM supply base
),

staking_rewards_mint as (
  select
    block_date,
    cover_id,
    reward_amount_expected_total,
    tx_hash
  --from query_4067736 -- staking rewards base
  from nexusmutual_ethereum.staking_rewards
),

staking_rewards_mint_agg as (
  select
    block_date,
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
    block_date,
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
    block_date,
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
    block_date,
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
    block_date,
    sum(amount) as nxm_gov_rewards
  from gov_rewards
  group by 1
),

capital_movement as (
  select
    date_trunc('day', block_time) as block_date,
    -1 * coalesce(sum(case when token_in = 'NXM' then amount_in end), 0) as nxm_withdrawal,
    coalesce(sum(case when token_out = 'NXM' then amount_out end), 0) as nxm_contribution
  from query_4498669 -- RAMM swaps - base
  group by 1
)

select
  ns.block_date,
  ns.nxm_supply,
  ns.nxm_mint,
  ns.nxm_burn,
  srm.nxm_reward_mint,
  coalesce(cbm.nxm_cover_buy_burn, 0) as nxm_cover_buy_burn,
  coalesce(sbc.nxm_claim_burn, 0) as nxm_claim_burn,
  coalesce(ar.nxm_assessor_rewards, 0) as nxm_assessor_rewards,
  coalesce(gr.nxm_gov_rewards, 0) as nxm_gov_rewards,
  cm.nxm_contribution,
  cm.nxm_withdrawal
from nxm_supply ns
  left join staking_rewards_mint_agg srm on ns.block_date = srm.block_date
  left join cover_buy_burn_agg cbm on ns.block_date = cbm.block_date
  left join capital_movement cm on ns.block_date = cm.block_date
  left join stake_burn_for_claims_agg sbc on ns.block_date = sbc.block_date
  left join assessor_rewards_agg ar on ns.block_date = ar.block_date
  left join gov_rewards_agg gr on ns.block_date = gr.block_date
where ns.block_date between timestamp '2024-12-31' and timestamp '2025-01-31'
order by 1 desc
