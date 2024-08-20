with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

products as (
  select
    p.product_id,
    p.product_name,
    pt.product_type_id,
    pt.product_type_name as product_type
  --from query_3788361 pt -- product types
  --  inner join query_3788363 p -- products
  from nexusmutual_ethereum.product_types_v2 pt
    inner join nexusmutual_ethereum.products_v2 p
      on pt.product_type_id = p.product_type_id
),

staking_pool_base as (
  select
    sp.pool_id,
    sp.pool_address,
    spn.pool_name,
    sp.manager_address,
    sp.manager_ens,
    sp.manager,
    sp.is_private_pool,
    sp.initial_pool_fee,
    sp.current_pool_fee,
    sp.max_management_fee,
    sp.product_id,
    p.product_type,
    p.product_name,
    sp.initial_price,
    sp.target_price,
    sp.initial_weight,
    sp.target_weight,
    sp.pool_created_time,
    sp.product_added_time
  --from query_3859935 sp -- staking pools base (fallback) query
  from nexusmutual_ethereum.staking_pools sp
    inner join products p on sp.product_id = p.product_id
    left join staking_pool_names spn on sp.pool_id = spn.pool_id
),

staking_pool_products as (
  select
    pool_id,
    pool_address,
    pool_name,
    product_id,
    product_name,
    product_type,
    coalesce(target_weight, initial_weight) as target_weight,
    initial_price,
    target_price,
    product_added_time
  from staking_pool_base
  where coalesce(target_weight, initial_weight) > 0
),

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    sp.pool_name,
    sp.pool_created_time,
    if(sp.is_private_pool, 'Private', 'Public') as pool_type,
    sp.manager,
    sp.initial_pool_fee / 100.00 as initial_pool_fee,
    sp.current_pool_fee / 100.00 as current_management_fee,
    sp.max_management_fee / 100.00 as max_management_fee,
    spp.total_weight / 100.00 as leverage,
    spp.product_count
  from staking_pool_base sp
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
    sp.pool_id,
    t.pool_address,
    sum(t.amount) as total_nxm_staked
  from (
      select
        pool_address,
        sum(total_amount) as amount
      --from query_3619534 -- staking deposit extensions base query -- can't be used bc of "Error: This query has too many stages and is too complex to execute at once"
      from nexusmutual_ethereum.staking_deposit_extensions
      where is_active
      group by 1
      union all
      select
        pool_address,
        sum(amount) as amount
      --from query_3609519 -- staking events
      from nexusmutual_ethereum.staking_events
      where is_active
        --and flow_type in ('withdraw', 'stake burn')
        and flow_type = 'withdraw' -- burn TBD
      group by 1
    ) t
    join staking_pools sp on t.pool_address = sp.pool_address
  group by 1, 2
),

latest_avg_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  from nexusmutual_ethereum.capital_pool_prices
  order by 1 desc
  limit 1
),

staked_nxm_allocated as (
  select
    sp.pool_id,
    sp.product_id,
    sp.target_weight / 100.00 as target_weight,
    sp.target_price,
    --s.total_nxm_staked, -- per pool
    --sum(sp.target_weight * s.total_nxm_staked / 100.00) over (partition by sp.pool_id) as nxm_allocated_total, -- per pool
    (sp.target_weight * s.total_nxm_staked / 100.00) as nxm_allocated,
    (sp.target_weight * s.total_nxm_staked / 100.00) * p.avg_nxm_usd_price as nxm_usd_allocated,
    (sp.target_weight * s.total_nxm_staked / 100.00) * p.avg_nxm_eth_price as nxm_eth_allocated,
    sp.product_added_time
  from staking_pool_products sp
    inner join staked_nxm_per_pool s on sp.pool_id = s.pool_id
    cross join latest_avg_prices p
),

active_covers as (
  select
    staking_pool_id,
    product_id,
    count(*) as active_cover_count,
    sum(eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount) as usd_cover_amount,
    sum(eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount) as eth_cover_amount
  from query_3834200 -- active covers base (fallback) query
  --from nexusmutual_ethereum.active_covers
  group by 1, 2
)

select
  spp.pool_id,
  --spp.pool_name,
  spp.product_id,
  spp.product_name,
  sna.target_weight,
  sna.nxm_allocated,
  sna.nxm_usd_allocated,
  sna.nxm_eth_allocated,
  ac.active_cover_count,
  ac.usd_cover_amount,
  ac.eth_cover_amount
from staking_pool_products spp
  left join staked_nxm_allocated sna on spp.pool_id = sna.pool_id and spp.product_id = sna.product_id
  left join active_covers ac on spp.pool_id = ac.staking_pool_id and spp.product_id = ac.product_id
where spp.pool_id = 3 -- 18
order by 1, 2
