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

staked_per_staker as (
  select
    staker,
    case '{{currency}}'
      when 'NXM' then staked_nxm
      when 'ETH' then staked_nxm_eth
      when 'USD' then staked_nxm_usd
    end as staked
  from stakers
),

bins as (
  select
    if(floor(log(2, staked)) < 0, -1, floor(log(2, staked))) as bin_floor,
    count(distinct staker) as staker_count
  from staked_per_staker
  group by 1
)

select
  bin_floor,
  format_number(power(2, bin_floor)) || ' - ' || format_number(power(2, bin_floor + 1)) as bin_range,
  staker_count
from bins
order by 1
