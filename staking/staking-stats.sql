select
  count(*) as pool_count,
  sum(total_allocated_nxm) / sum(total_staked_nxm) as leverage,
  sum(case '{{currency}}'
        when 'NXM' then total_staked_nxm
        when 'ETH' then total_staked_nxm_eth
        when 'USD' then total_staked_nxm_usd
      end) as total_staked,
  sum(case '{{currency}}'
        when 'NXM' then total_allocated_nxm
        when 'ETH' then total_allocated_nxm_eth
        when 'USD' then total_allocated_nxm_usd
      end) as total_allocated,
  sum(case '{{currency}}'
        when 'NXM' then reward_current_total_nxm
        when 'ETH' then reward_current_total_nxm_eth
        when 'USD' then reward_current_total_nxm_usd
      end) as reward_current_total,
  sum(case '{{currency}}'
        when 'NXM' then reward_expected_total_nxm
        when 'ETH' then reward_expected_total_nxm_eth
        when 'USD' then reward_expected_total_nxm_usd
      end) as reward_expected_total,
  sum(case '{{currency}}'
        when 'NXM' then pool_manager_commission_nxm
        when 'ETH' then pool_manager_commission_nxm_eth
        when 'USD' then pool_manager_commission_nxm_usd
      end) as pool_manager_commission,
  sum(case '{{currency}}'
        when 'NXM' then pool_distributor_commission_nxm
        when 'ETH' then pool_distributor_commission_nxm_eth
        when 'USD' then pool_distributor_commission_nxm_usd
      end) as pool_distributor_commission,
  sum(case '{{currency}}'
        when 'NXM' then staker_commission_nxm
        when 'ETH' then staker_commission_nxm_eth
        when 'USD' then staker_commission_nxm_usd
      end) as staker_commission,
  sum(case '{{currency}}'
        when 'NXM' then total_commission_nxm
        when 'ETH' then total_commission_nxm_eth
        when 'USD' then total_commission_nxm_usd
      end) as total_commission
from query_3599009 -- staking pools overview - base query
