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
  year(r.block_date) as year_date,
  r.pool_id,
  spn.pool_name,
  case '{{currency}}'
    when 'NXM' then sum(r.reward_total)
    when 'ETH' then sum(r.reward_total * p.avg_nxm_eth_price)
    when 'USD' then sum(r.reward_total * p.avg_nxm_usd_price)
  end as reward_total
from query_4068272 r -- daily staking rewards base query
  left join staking_pool_names spn on r.pool_id = spn.pool_id
  cross join latest_prices p
--where r.block_date >= date_add('year', -12, date_trunc('year', current_date))
group by 1, 2, 3
order by 1, 2
