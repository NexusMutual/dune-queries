with

stakers as (
  select
    staker,
    sum(staked_nxm) as staked_nxm,
    sum(staked_nxm_eth) as staked_nxm_eth,
    sum(staked_nxm_usd) as staked_nxm_usd
  from query_4077503 -- stakers - base
  --where staker <> '0x84edffa16bb0b9ab1163abb0a13ff0744c11272f' -- legacy pooled staking v1
  group by 1
),

-- temp bodge to account for burns until deposit updates cover all active tranches for all tokens
burns as (
  select sum(amount) as burned_nxm
  from nexusmutual_ethereum.staking_events
  where flow_type = 'stake burn'
    and pool_id <> 1
)

select
  count(distinct staker) as unique_stakers,
  min(case '{{currency}}'
    when 'NXM' then staked_nxm
    when 'ETH' then staked_nxm_eth
    when 'USD' then staked_nxm_usd
  end) as staked_min,
  avg(case '{{currency}}'
    when 'NXM' then staked_nxm
    when 'ETH' then staked_nxm_eth
    when 'USD' then staked_nxm_usd
  end) as staked_avg,
  max(case '{{currency}}'
    when 'NXM' then staked_nxm
    when 'ETH' then staked_nxm_eth
    when 'USD' then staked_nxm_usd
  end) as staked_max,
  sum(case '{{currency}}'
    when 'NXM' then staked_nxm
    when 'ETH' then staked_nxm_eth
    when 'USD' then staked_nxm_usd
  end) + min(if('{{currency}}' = 'NXM', burned_nxm, 0)) as staked_total
from stakers, burns
