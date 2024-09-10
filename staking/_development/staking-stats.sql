select
  count(*) as pool_count,
  sum(total_nxm_staked) as total_nxm_staked,
  sum(total_nxm_allocated) as total_nxm_allocated,
  sum(total_nxm_allocated) / sum(total_nxm_staked) as leverage,
  sum(reward_amount_current_total) as reward_amount_current_total,
  sum(reward_amount_expected_total) as reward_amount_expected_total,
  sum(pool_manager_commission) as pool_manager_commission,
  sum(pool_distributor_commission) as pool_distributor_commission,
  sum(staker_commission) as staker_commission,
  sum(total_commission) as total_commission
from query_3599009 -- staking pools overview
