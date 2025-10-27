with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

staking_pool_products as (
  select
    pool_id,
    pool_address,
    product_id,
    coalesce(target_weight, initial_weight) as target_weight
  --from query_3859935 -- staking pools base (fallback) query
  --from query_4167546 -- staking pools - spell de-duped
  from nexusmutual_ethereum.staking_pools -- spell checks for dupes
),

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    sp.pool_created_time,
    sp.is_private_pool,
    sp.manager,
    sp.initial_pool_fee,
    sp.current_management_fee,
    sp.max_management_fee,
    spp.total_weight as leverage,
    spp.product_count
  --from query_3859935 sp -- staking pools base (fallback) query
  --from query_4167546 sp -- staking pools - spell de-duped
  from nexusmutual_ethereum.staking_pools sp -- spell checks for dupes
    inner join (
      select
        pool_id,
        count(distinct product_id) filter (where target_weight > 0) as product_count,
        sum(target_weight) as total_weight
      from staking_pool_products
      group by 1
    ) spp on sp.pool_id = spp.pool_id
),

staked_nxm_per_pool as (
  select
    pool_id,
    total_staked_nxm
  --from query_4065286 -- staked nxm per pool - base
  --from nexusmutual_ethereum.staked_per_pool
  from dune.nexus_mutual.result_staked_nxm_per_pool
  where pool_date_rn = 1
),

staked_nxm_allocated as (
  select
    w.pool_id,
    sum(w.target_weight * s.total_staked_nxm) as total_allocated_nxm
  from staking_pool_products w
    inner join staked_nxm_per_pool s on w.pool_id = s.pool_id
  group by 1
),

current_rewards as (
  select
    pool_id,
    sum(reward_total) as reward_total,
    min_by(
      case
        when block_date = current_date and reward_total > 0 then reward_total
        else 0
      end,
      pool_date_rn
    ) as latest_daily_reward_total -- min bc pool_date_rn is desc
  from query_4068272 -- daily staking rewards - base
  group by 1
),

expected_rewards as (
  select
    pool_id,
    sum(reward_expected_total) as reward_expected_total
  from query_4068262 -- expected staking rewards - base
  group by 1
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_eth_usd_price, block_date) as avg_eth_usd_price,
    max_by(avg_dai_usd_price, block_date) as avg_dai_usd_price,
    max_by(avg_usdc_usd_price, block_date) as avg_usdc_usd_price,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
)

select
  sp.pool_id,
  spn.pool_name,
  sp.manager,
  if(sp.is_private_pool, 'Private', 'Public') as pool_type,
  coalesce(dr.latest_daily_reward_total, 0) / nullif(t.total_staked_nxm, 0) * 365.0 as apy,
  sp.current_management_fee,
  sp.max_management_fee,
  sp.leverage,
  sp.product_count,
  -- staked
  coalesce(t.total_staked_nxm, 0) as total_staked_nxm,
  coalesce(t.total_staked_nxm * p.avg_nxm_usd_price, 0) as total_staked_nxm_usd,
  coalesce(t.total_staked_nxm * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as total_staked_nxm_eth,
  -- allocated
  coalesce(a.total_allocated_nxm, 0) as total_allocated_nxm,
  coalesce(a.total_allocated_nxm * p.avg_nxm_usd_price, 0) as total_allocated_nxm_usd,
  coalesce(a.total_allocated_nxm * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as total_allocated_nxm_eth,
  -- rewards
  coalesce(dr.reward_total, 0) as reward_current_total_nxm,
  coalesce(dr.reward_total * p.avg_nxm_usd_price, 0) as reward_current_total_nxm_usd,
  coalesce(dr.reward_total * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as reward_current_total_nxm_eth,
  coalesce(er.reward_expected_total, 0) as reward_expected_total_nxm,
  coalesce(er.reward_expected_total * p.avg_nxm_usd_price, 0) as reward_expected_total_nxm_usd,
  coalesce(er.reward_expected_total * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as reward_expected_total_nxm_eth
from staking_pools sp
  left join staking_pool_names spn on sp.pool_id = spn.pool_id
  left join staked_nxm_per_pool t on sp.pool_id = t.pool_id
  left join staked_nxm_allocated a on sp.pool_id = a.pool_id
  left join current_rewards dr on sp.pool_id = dr.pool_id
  left join expected_rewards er on sp.pool_id = er.pool_id
  cross join latest_prices p
--order by 1
