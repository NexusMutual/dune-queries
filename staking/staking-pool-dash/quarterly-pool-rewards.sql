with

params as (
  select cast(split_part('{{pool}}', ' : ', 1) as int) as pool_id
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
  date_trunc('quarter', r.block_date) as quarter_date,
  case '{{currency}}'
    when 'NXM' then sum(r.reward_total)
    when 'ETH' then sum(r.reward_total * p.avg_nxm_eth_price)
    when 'USD' then sum(r.reward_total * p.avg_nxm_usd_price)
  end as reward_total
from query_4068272 r -- daily staking rewards base query
  cross join latest_prices p
where cast(r.pool_id as int) in (select pool_id from params)
group by 1
order by 1
