select
  block_month,
  nxm_supply_start,
  nxm_supply_end,
  nxm_cover_buy_burn,
  nxm_claim_burn,
  nxm_reward_mint,
  nxm_assessor_rewards,
  nxm_gov_rewards,
  nxm_reward_mint + nxm_assessor_rewards + nxm_gov_rewards as nxm_reward_mint_total,
  nxm_contribution,
  nxm_withdrawal,
  nxm_reward_mint + nxm_assessor_rewards + nxm_gov_rewards + nxm_contribution as nxm_mint_total,
  nxm_cover_buy_burn + nxm_claim_burn + nxm_withdrawal as nxm_burn_total
from query_4911759 -- nxm movement
where block_month >= date_add('month', -12, current_date)
order by 1 desc
