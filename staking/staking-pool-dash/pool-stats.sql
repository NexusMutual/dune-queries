with

params as (
  select cast(split_part('{{pool}}', ' : ', 1) as int) as pool_id
),

stakers as (
  select count(distinct staker) as unique_stakers
  from query_4077503 -- stakers active stake - base
  where cast(pool_id as int) in (select pool_id from params)
)

select
  spo.pool_id,
  spo.pool_name,
  spo.apy * 100.00 as apy,
  spo.manager,
  spo.pool_type,
  spo.current_management_fee * 100.00 as current_management_fee,
  spo.max_management_fee * 100.00 as max_management_fee,
  spo.leverage,
  spo.product_count,
  s.unique_stakers,
  case '{{currency}}' 
    when 'NXM' then spo.total_staked_nxm
    when 'ETH' then spo.total_staked_nxm_eth
    when 'USD' then spo.total_staked_nxm_usd
  end as total_staked,
  case '{{currency}}'
    when 'NXM' then spo.total_allocated_nxm
    when 'ETH' then spo.total_allocated_nxm_eth
    when 'USD' then spo.total_allocated_nxm_usd
  end as total_allocated,
  case '{{currency}}'
    when 'NXM' then spo.reward_current_total_nxm
    when 'ETH' then spo.reward_current_total_nxm_eth
    when 'USD' then spo.reward_current_total_nxm_usd
  end as reward_current_total,
  case '{{currency}}'
    when 'NXM' then spo.reward_expected_total_nxm
    when 'ETH' then spo.reward_expected_total_nxm_eth
    when 'USD' then spo.reward_expected_total_nxm_usd
  end as reward_expected_total
from query_3599009 spo -- staking pools overview - base query
  cross join stakers s
where cast(spo.pool_id as int) in (select pool_id from params)
