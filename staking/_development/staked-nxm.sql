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

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    s.block_date
  from staking_pools sp
    cross join unnest (
      sequence(
        cast(date_trunc('day', sp.pool_created_time) as timestamp),
        cast(date_trunc('day', now()) as timestamp),
        interval '1' day
      )
    ) as s(block_date)
),

staked_nxm_per_pool as (
  select
    block_date,
    pool_id,
    sum(total_amount) as total_nxm_staked,
    dense_rank() over (partition by pool_id order by block_date desc) as rn
  from (
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        sum(se.total_amount) as total_amount
      from staking_pool_day_sequence d
        left join query_3619534 se -- staking deposit extensions base query
        --left join nexusmutual_ethereum.staking_deposit_extensions se
          on d.pool_address = se.pool_address
         --and d.block_date = date_trunc('day', se.block_time)
         --and d.block_date <= se.tranche_expiry_date
         and d.block_date between date_trunc('day', se.block_time) and se.tranche_expiry_date
      group by 1,2
      union all
      -- withdrawals (& burns?)
      select
        d.block_date,
        d.pool_id,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        left join query_3609519 se -- staking events
        --left join nexusmutual_ethereum.staking_events se
          on d.pool_address = se.pool_address
         --and d.block_date = date_trunc('day', se.block_time)
         --and d.block_date <= se.tranche_expiry_date
         and d.block_date between date_trunc('day', se.block_time) and se.tranche_expiry_date
      where 1=1
        --and flow_type in ('withdraw', 'stake burn')
        and flow_type = 'withdraw' -- burn TBD
      group by 1,2
    ) t
  group by 1, 2
)

select * 
from staked_nxm_per_pool
where pool_id = 3
order by 1
