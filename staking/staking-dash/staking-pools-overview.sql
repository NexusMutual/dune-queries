with staking_pools_overview as (
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
)

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
  total_staked,
  total_allocated,
  earned_reward_total,
  if(abs(pending_reward_total) > 1e-10, pending_reward_total, 0) as pending_reward_total,
  aggregate_reward_total
from staking_pools_overview
order by 1
