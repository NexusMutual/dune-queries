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
  s.pool_id,
  s.block_date,
  spn.pool_name,
  case '{{currency}}'
    when 'NXM' then s.total_staked_nxm
    when 'ETH' then s.total_staked_nxm * p.avg_nxm_eth_price
    when 'USD' then s.total_staked_nxm * p.avg_nxm_usd_price
  end as total_staked
--from query_4065286 s -- staked nxm base query (uses staking pools - spell de-duped)
from nexusmutual_ethereum.staked_per_pool s
  left join staking_pool_names spn on s.pool_id = spn.pool_id
  cross join latest_prices p
order by 1, 2
