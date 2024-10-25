with

stakers as (
  select
    staker,
    sum(staked_nxm) as staked_nxm,
    sum(staked_nxm_eth) as staked_nxm_eth,
    sum(staked_nxm_usd) as staked_nxm_usd,
    listagg(cast(pool_id as varchar), ',') within group (order by pool_id) as pools,
    listagg(cast(token_id as varchar), ',') within group (order by token_id) as tokens
  from query_4077503 -- stakers - base
  --where staker <> '0x84edffa16bb0b9ab1163abb0a13ff0744c11272f' -- legacy pooled staking v1
  group by 1
),

stakers_output as (
  select
    staker,
    case '{{currency}}'
      when 'NXM' then staked_nxm
      when 'ETH' then staked_nxm_eth
      when 'USD' then staked_nxm_usd
    end as staked,
    pools,
    tokens
  from stakers
)

select
  staker,
  staked,
  round(staked / (select sum(staked) from stakers_output), 6) as pct_total_staked,
  pools,
  tokens
from stakers_output
order by 2 desc
