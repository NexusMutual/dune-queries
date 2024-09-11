select
  count(*) as pool_count,
  sum(total_staked_nxm) as total_staked_nxm,
  sum(total_allocated_nxm) as total_allocated_nxm,
  sum(total_allocated_nxm) / sum(total_staked_nxm) as leverage,
  sum(reward_current_total_nxm) as reward_current_total_nxm,
  sum(reward_expected_total_nxm) as reward_expected_total_nxm,
  sum(pool_manager_commission_nxm) as pool_manager_commission_nxm,
  sum(pool_distributor_commission_nxm) as pool_distributor_commission_nxm,
  sum(staker_commission_nxm) as staker_commission_nxm,
  sum(total_commission_nxm) as total_commission_nxm
from query_3599009 -- staking pools overview
