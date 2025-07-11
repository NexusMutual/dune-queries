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
    product_name,
    product_type,
    coalesce(target_weight, initial_weight) as target_weight,
    initial_price,
    target_price,
    product_added_time
  --from query_3859935 -- staking pools base (fallback) query
  --from query_4167546 -- staking pools - spell de-duped
  from nexusmutual_ethereum.staking_pools -- spell checks for dupes
  where coalesce(target_weight, initial_weight) > 0
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
        count(distinct product_id) as product_count,
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
  from nexusmutual_ethereum.staked_per_pool
  where pool_date_rn = 1
),

staked_nxm_allocated as (
  select
    sp.pool_id,
    sp.product_id,
    sp.target_weight,
    sp.target_price,
    s.total_staked_nxm,
    sp.target_weight * s.total_staked_nxm as allocated_nxm,
    sp.product_added_time
  from staking_pool_products sp
    inner join staked_nxm_per_pool s on sp.pool_id = s.pool_id
),

active_bucket as (
  select
    staking_pool_id,
    product_id,
    count(*) as cover_count,
    sum(nxm_cover_amount_per_pool) as nxm_cover_amount
  from query_4703493 -- active bucket - base (fallback query)
  group by 1, 2
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback query)
  from nexusmutual_ethereum.capital_pool_prices
),

staking_totals_combined as (
  select
    spp.pool_id,
    spn.pool_name,
    spp.product_id,
    spp.product_name,
    spp.product_type,
    sna.target_weight,
    -- staked
    coalesce(sna.total_staked_nxm, 0) as staked_nxm,
    coalesce(sna.total_staked_nxm * p.avg_nxm_usd_price, 0) as staked_nxm_usd,
    coalesce(sna.total_staked_nxm * p.avg_nxm_eth_price, 0) as staked_nxm_eth,
    -- allocated
    coalesce(sna.allocated_nxm, 0) as allocated_nxm,
    coalesce(sna.allocated_nxm * p.avg_nxm_usd_price, 0) as allocated_nxm_usd,
    coalesce(sna.allocated_nxm * p.avg_nxm_eth_price, 0) as allocated_nxm_eth,
    -- active bucket
    coalesce(ab.cover_count, 0) as cover_count_per_active_bucket,
    coalesce(ab.nxm_cover_amount * p.avg_nxm_usd_price, 0) as usd_cover_amount,
    coalesce(ab.nxm_cover_amount * p.avg_nxm_eth_price, 0) as eth_cover_amount,
    coalesce(ab.nxm_cover_amount, 0) as nxm_cover_amount
  from staking_pool_products spp
    inner join staked_nxm_allocated sna on spp.pool_id = sna.pool_id and spp.product_id = sna.product_id
    left join active_bucket ab on spp.pool_id = ab.staking_pool_id and spp.product_id = ab.product_id
    left join staking_pool_names spn on spp.pool_id = spn.pool_id
    cross join latest_prices p
)

select
  pool_id,
  pool_name,
  product_id,
  product_name,
  product_type,
  target_weight,
  -- staked
  staked_nxm,
  staked_nxm_usd,
  staked_nxm_eth,
  -- allocated
  2 * allocated_nxm as max_capacity_nxm,
  2 * allocated_nxm_usd as max_capacity_nxm_usd,
  2 * allocated_nxm_eth as max_capacity_nxm_eth,
  -- active bucket
  cover_count_per_active_bucket,
  nxm_cover_amount,
  usd_cover_amount,
  eth_cover_amount,
  -- available capacity
  if((2 * allocated_nxm) - nxm_cover_amount < 0, 0, (2 * allocated_nxm) - nxm_cover_amount) as available_capacity_nxm,
  if((2 * allocated_nxm_usd) - usd_cover_amount < 0, 0, (2 * allocated_nxm_usd) - usd_cover_amount) as available_capacity_usd,
  if((2 * allocated_nxm_eth) - eth_cover_amount < 0, 0, (2 * allocated_nxm_eth) - eth_cover_amount) as available_capacity_eth
from staking_totals_combined
--order by 1, 3
