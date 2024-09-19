with

staked_per_staker as (
  select
    staker,
    case '{{currency}}'
      when 'NXM' then staked_nxm
      when 'ETH' then staked_nxm_eth
      when 'USD' then staked_nxm_usd
    end as staked
  from query_4077503 -- stakers - base
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
