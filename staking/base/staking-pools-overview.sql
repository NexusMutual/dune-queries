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
  from nexusmutual_ethereum.staking_pools
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
  from nexusmutual_ethereum.staking_pools sp
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
  from query_4065286 -- staked nxm base query
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

staking_pool_fee_history as (
  select
    sp.pool_id,
    sp.pool_address,
    t.start_time,
    coalesce(
      date_add('second', -1, lead(t.start_time) over (partition by t.pool_address order by t.start_time)),
      now()
    ) as end_time,
    t.pool_fee
  from staking_pools sp
    inner join (
      select
        pool_address,
        pool_created_time as start_time,
        initial_pool_fee as pool_fee
      from staking_pools
      union all
      select
        contract_address as pool_address,
        evt_block_time as start_time,
        newFee / 100.00 as pool_fee
      from nexusmutual_ethereum.StakingPool_evt_PoolFeeChanged
    ) t on sp.pool_address = t.pool_address
),

covers as (
  select *
  from nexusmutual_ethereum.covers_v2
  --from query_3788370 -- covers v2 base (fallback) query
),

cover_premiums as (
  select
    c.cover_id,
    c.cover_start_time,
    c.cover_end_time,
    c.staking_pool_id,
    c.product_id,
    c.premium,
    c.commission as cover_commision,
    c.premium * 0.5 as commission,
    c.commission_ratio * c.premium * 0.5 as commission_distibutor_fee,
    c.premium * 0.5 * h.pool_fee as pool_manager_commission,
    c.premium * 0.5 * (1 - h.pool_fee) as staker_commission,
    c.premium_period_ratio * c.premium * 0.5 as commission_emitted,
    c.premium_period_ratio * c.premium * 0.5 * h.pool_fee as pool_manager_commission_emitted,
    c.premium_period_ratio * c.premium * 0.5 * (1 - h.pool_fee) as staker_commission_emitted
  from covers c
    inner join staking_pool_fee_history h on c.staking_pool_id = h.pool_id
      and c.cover_start_time between h.start_time and h.end_time
),

cover_premium_commissions as (
  select
    staking_pool_id,
    sum(commission) as total_commission,
    sum(commission_emitted) as total_commission_emitted,
    sum(commission_distibutor_fee) as pool_distributor_commission,
    sum(pool_manager_commission) as pool_manager_commission,
    sum(pool_manager_commission_emitted) as pool_manager_commission_emitted,
    sum(staker_commission) as staker_commission,
    sum(staker_commission_emitted) as staker_commission_emitted
  from cover_premiums
  group by 1
),

current_rewards as (
  select
    pool_id,
    sum(reward_total) as reward_total
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
  coalesce(er.reward_expected_total * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as reward_expected_total_nxm_eth,
  -- commisions
  coalesce(c.pool_manager_commission, 0) as pool_manager_commission_nxm,
  coalesce(c.pool_manager_commission, 0) as pool_manager_commission_nxm_usd,
  coalesce(c.pool_manager_commission * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as pool_manager_commission_nxm_eth,
  coalesce(c.pool_distributor_commission, 0) as pool_distributor_commission_nxm,
  coalesce(c.pool_distributor_commission * p.avg_nxm_usd_price, 0) as pool_distributor_commission_nxm_usd,
  coalesce(c.pool_distributor_commission * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as pool_distributor_commission_nxm_eth,
  coalesce(c.staker_commission, 0) as staker_commission_nxm,
  coalesce(c.staker_commission * p.avg_nxm_usd_price, 0) as staker_commission_nxm_usd,
  coalesce(c.staker_commission * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as staker_commission_nxm_eth,
  coalesce(c.pool_manager_commission, 0) + coalesce(c.pool_distributor_commission, 0) + coalesce(c.staker_commission, 0) as total_commission_nxm,
  (coalesce(c.pool_manager_commission, 0) + coalesce(c.pool_distributor_commission, 0) + coalesce(c.staker_commission, 0)) * p.avg_nxm_usd_price as total_commission_nxm_usd,
  (coalesce(c.pool_manager_commission, 0) + coalesce(c.pool_distributor_commission, 0) + coalesce(c.staker_commission, 0)) * p.avg_nxm_usd_price / p.avg_eth_usd_price as total_commission_nxm_eth,
  coalesce(c.staker_commission_emitted, 0) as staker_commission_emitted_nxm,
  coalesce(c.staker_commission_emitted * p.avg_nxm_usd_price, 0) as staker_commission_emitted_nxm_usd,
  coalesce(c.staker_commission_emitted * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as staker_commission_emitted_nxm_eth,
  coalesce(c.staker_commission, 0) - coalesce(c.staker_commission_emitted, 0) as future_staker_commission_nxm,
  (coalesce(c.staker_commission, 0) - coalesce(c.staker_commission_emitted, 0)) * p.avg_nxm_usd_price as future_staker_commission_nxm_usd,
  (coalesce(c.staker_commission, 0) - coalesce(c.staker_commission_emitted, 0)) * p.avg_nxm_usd_price / p.avg_eth_usd_price as future_staker_commission_nxm_eth,
  coalesce(c.pool_manager_commission_emitted, 0) as pool_manager_commission_emitted_nxm,
  coalesce(c.pool_manager_commission_emitted * p.avg_nxm_usd_price, 0) as pool_manager_commission_emitted_nxm_usd,
  coalesce(c.pool_manager_commission_emitted * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as pool_manager_commission_emitted_nxm_eth,
  coalesce(c.pool_manager_commission, 0) - coalesce(c.pool_manager_commission_emitted, 0) as future_pool_manager_commission_nxm,
  (coalesce(c.pool_manager_commission, 0) - coalesce(c.pool_manager_commission_emitted, 0)) * p.avg_nxm_usd_price as future_pool_manager_commission_nxm_usd,
  (coalesce(c.pool_manager_commission, 0) - coalesce(c.pool_manager_commission_emitted, 0)) * p.avg_nxm_usd_price / p.avg_eth_usd_price as future_pool_manager_commission_nxm_eth
from staking_pools sp
  left join staking_pool_names spn on sp.pool_id = spn.pool_id
  left join staked_nxm_per_pool t on sp.pool_id = t.pool_id
  left join staked_nxm_allocated a on sp.pool_id = a.pool_id
  left join current_rewards dr on sp.pool_id = dr.pool_id
  left join expected_rewards er on sp.pool_id = er.pool_id
  left join cover_premium_commissions c on sp.pool_id = c.staking_pool_id
  cross join latest_prices p
--order by 1
