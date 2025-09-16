with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

active_covers as (
  select
    ac.cover_id,
    ac.cover_start_date,
    ac.cover_end_date,
    ac.staking_pool_id,
    spn.pool_name as staking_pool,
    --ETH
    ac.eth_cover_amount,
    ac.eth_usd_cover_amount,
    --DAI
    ac.dai_eth_cover_amount,
    ac.dai_usd_cover_amount,
    --USDC
    ac.usdc_eth_cover_amount,
    ac.usdc_usd_cover_amount,
    --cbBTC
    ac.cbbtc_eth_cover_amount,
    ac.cbbtc_usd_cover_amount
  from query_5785377 ac -- active covers - base root
    left join staking_pool_names spn on ac.staking_pool_id = spn.pool_id
)

select
  staking_pool_id,
  staking_pool,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount + cbbtc_eth_cover_amount
  )) as cover_amount
from active_covers
group by 1, 2
order by 3 desc
