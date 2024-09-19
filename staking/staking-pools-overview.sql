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
  end reward_current_total,
  case '{{currency}}'
    when 'NXM' then reward_expected_total_nxm
    when 'ETH' then reward_expected_total_nxm_eth
    when 'USD' then reward_expected_total_nxm_usd
  end reward_expected_total
  /*
  case '{{currency}}'
    when 'NXM' then pool_manager_commission_nxm
    when 'ETH' then pool_manager_commission_nxm_eth
    when 'USD' then pool_manager_commission_nxm_usd
  end as pool_manager_commission,
  case '{{currency}}'
    when 'NXM' then pool_distributor_commission_nxm
    when 'ETH' then pool_distributor_commission_nxm_eth
    when 'USD' then pool_distributor_commission_nxm_usd
  end as pool_distributor_commission,
  case '{{currency}}'
    when 'NXM' then staker_commission_nxm
    when 'ETH' then staker_commission_nxm_eth
    when 'USD' then staker_commission_nxm_usd
  end as staker_commission,
  case '{{currency}}'
    when 'NXM' then total_commission_nxm
    when 'ETH' then total_commission_nxm_eth
    when 'USD' then total_commission_nxm_usd
  end as total_commission,
  case '{{currency}}'
    when 'NXM' then staker_commission_emitted_nxm
    when 'ETH' then staker_commission_emitted_nxm_eth
    when 'USD' then staker_commission_emitted_nxm_usd
  end as staker_commission_emitted,
  case '{{currency}}'
    when 'NXM' then future_staker_commission_nxm
    when 'ETH' then future_staker_commission_nxm_eth
    when 'USD' then future_staker_commission_nxm_usd
  end as future_staker_commission,
  case '{{currency}}'
    when 'NXM' then pool_manager_commission_emitted_nxm
    when 'ETH' then pool_manager_commission_emitted_nxm_eth
    when 'USD' then pool_manager_commission_emitted_nxm_usd
  end as pool_manager_commission_emitted,
  case '{{currency}}'
    when 'NXM' then future_pool_manager_commission_nxm
    when 'ETH' then future_pool_manager_commission_nxm_eth
    when 'USD' then future_pool_manager_commission_nxm_usd
  end as future_pool_manager_commission
  */
from query_3599009 -- staking pools overview - base query
order by 1
