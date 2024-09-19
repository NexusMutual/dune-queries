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
      end) as reward_expected_total
from query_3599009 -- staking pools overview - base query
