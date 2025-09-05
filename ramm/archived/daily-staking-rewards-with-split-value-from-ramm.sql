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
),

staking_rewards as (
  select
    r.pool_id,
    r.block_date,
    spn.pool_name,
    r.reward_total as nxm_reward_total,
    r.reward_total * p.avg_nxm_eth_price as eth_reward_total,
    r.reward_total * p.avg_nxm_usd_price as usd_reward_total
  from query_4068272 r -- daily staking rewards base query
    left join staking_pool_names spn on r.pool_id = spn.pool_id
    cross join latest_prices p
  where r.block_date >= timestamp '2024-01-01'
),

ramm_added as (
  select
    block_date,
    bv_diff_eth,
    bv_diff_nxm,
    bv_diff_usd
  from query_5730042 -- ramm daily value accrual
  where block_date >= timestamp '2024-01-01'
),

daily_denoms as (
  select
    block_date,
    sum(case when usd_reward_total > 0 then usd_reward_total else 0 end) as denom_usd
  from staking_rewards
  group by 1
),

pool_weights as (
  select
    sr.block_date,
    sr.pool_id,
    sr.pool_name,
    case
      when sr.usd_reward_total > 0 and md.denom_usd > 0 then sr.usd_reward_total / md.denom_usd
      else 0
    end as usd_weight
  from staking_rewards sr
    inner join daily_denoms md on md.block_date = sr.block_date
),

ramm_alloc as (
  select
    pw.block_date,
    pw.pool_id,
    pw.usd_weight * ra.bv_diff_nxm as add_nxm,
    pw.usd_weight * ra.bv_diff_eth as add_eth,
    pw.usd_weight * ra.bv_diff_usd as add_usd
  from pool_weights pw
    left join ramm_added ra on ra.block_date = pw.block_date
)

select
  sr.block_date,
  sr.pool_id,
  sr.pool_name,
  sr.nxm_reward_total + coalesce(ra.add_nxm, 0) as nxm_reward_total,
  sr.eth_reward_total + coalesce(ra.add_eth, 0) as eth_reward_total,
  sr.usd_reward_total + coalesce(ra.add_usd, 0) as usd_reward_total
from staking_rewards sr
  left join ramm_alloc ra on ra.block_date = sr.block_date and ra.pool_id = sr.pool_id
order by 1, 2
