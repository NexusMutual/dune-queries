select
  staker,
  case '{{currency}}'
    when 'NXM' then staked_nxm
    when 'ETH' then staked_nxm_eth
    when 'USD' then staked_nxm_usd
  end as staked,
  pools,
  tokens
from query_4077503 -- stakers - base
order by 2 desc
