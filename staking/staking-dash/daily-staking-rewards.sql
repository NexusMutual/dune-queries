with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
)

select
  r.pool_id,
  r.block_date,
  spn.pool_name,
  case '{{currency}}'
    when 'NXM' then r.reward_total
    when 'ETH' then r.reward_total * p.avg_nxm_eth_price
    when 'USD' then r.reward_total * p.avg_nxm_usd_price
  end as reward_total
from query_4068272 r -- daily staking rewards base query
  left join staking_pool_names spn on r.pool_id = spn.pool_id
  cross join latest_prices p
order by 1, 2