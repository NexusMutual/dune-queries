select
  pool_id,
  pool_name,
  manager,
  pool_type,
  apy,
  current_management_fee,
  max_management_fee,
  leverage,
  product_count,
  case '{{currency}}'
    when 'NXM' then total_staked_nxm
    when 'ETH' then total_staked_nxm_eth
    when 'USD' then total_staked_nxm_usd
  end as total_staked,
  case '{{currency}}'
    when 'NXM' then total_allocated_nxm
    when 'ETH' then total_allocated_nxm_eth
    when 'USD' then total_allocated_nxm_usd
  end total_allocated,
  case '{{currency}}'
    when 'NXM' then reward_current_total_nxm
    when 'ETH' then reward_current_total_nxm_eth
    when 'USD' then reward_current_total_nxm_usd
  end earned_reward_total,
  case '{{currency}}'
    when 'NXM' then reward_expected_total_nxm - reward_current_total_nxm
    when 'ETH' then reward_expected_total_nxm_eth - reward_current_total_nxm_eth
    when 'USD' then reward_expected_total_nxm_usd - reward_current_total_nxm_usd
  end pending_reward_total,
  case '{{currency}}'
    when 'NXM' then reward_expected_total_nxm
    when 'ETH' then reward_expected_total_nxm_eth
    when 'USD' then reward_expected_total_nxm_usd
  end aggregate_reward_total
from query_3599009 -- staking pools overview - base query
order by 1
