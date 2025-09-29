with

stakers_base as (
  select
    pool_id,
    count(distinct staker) as unique_stakers
  from query_5578777 -- stakers - base
  where pool_token_rn = 1 -- current staker
  group by 1
)

select
  sp.pool_id,
  sp.pool_name,
  sp.pool_type,
  sp.apy,
  sb.unique_stakers,
  sp.total_staked_nxm,
  sp.total_staked_nxm_usd,
  sp.reward_current_total_nxm,
  sp.reward_current_total_nxm_usd,
  sp.reward_expected_total_nxm,
  sp.reward_expected_total_nxm_usd
from query_3599009 sp -- staking pools overview - base
  left join stakers_base sb on sp.pool_id = sb.pool_id
order by 1
