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
),

daily_stake as (
  select
    s.pool_id,
    s.block_date,
    case '{{currency}}'
      when 'NXM' then s.total_staked_nxm
      when 'ETH' then s.total_staked_nxm * p.avg_nxm_eth_price
      when 'USD' then s.total_staked_nxm * p.avg_nxm_usd_price
    end as total_staked
  --from query_4065286 s -- staked nxm base query (uses staking pools - spell de-duped)
  from nexusmutual_ethereum.staked_per_pool s
    cross join latest_prices p
  where cast(s.pool_id as int) in (select pool_id from params)
),

daily_rewards as (
  select
    r.pool_id,
    r.block_date,
    case '{{currency}}'
      when 'NXM' then r.reward_total
      when 'ETH' then r.reward_total * p.avg_nxm_eth_price
      when 'USD' then r.reward_total * p.avg_nxm_usd_price
    end as reward_total
  from query_4068272 r -- daily staking rewards base query
    cross join latest_prices p
  where cast(r.pool_id as int) in (select pool_id from params)
)

select
  ds.block_date,
  ds.total_staked,
  dr.reward_total,
  dr.reward_total / nullif(ds.total_staked, 0) * 36500.0 as apy
from daily_stake ds
  left join daily_rewards dr on ds.pool_id = dr.pool_id and ds.block_date = dr.block_date
order by 1
