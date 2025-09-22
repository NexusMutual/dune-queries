with

stakers as (
  select
    pool_id,
    pool_name,
    staker,
    tranche_id,
    stake_expiry_date,
    sum(staked_nxm) as staked_nxm,
    sum(staked_nxm_eth) as staked_nxm_eth,
    sum(staked_nxm_usd) as staked_nxm_usd
  from query_4077503 -- stakers active stake - base
  --where staker <> '0x84edffa16bb0b9ab1163abb0a13ff0744c11272f' -- legacy pooled staking v1
  group by 1, 2, 3, 4, 5
)

select
  stake_expiry_date,
  pool_name,
  sum(case '{{currency}}'
    when 'NXM' then staked_nxm
    when 'ETH' then staked_nxm_eth
    when 'USD' then staked_nxm_usd
  end) over (order by stake_expiry_date desc) as staked
from stakers
order by 1
