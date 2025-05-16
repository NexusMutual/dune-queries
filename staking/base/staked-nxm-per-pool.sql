with

staking_pools as (
  select
    sp.pool_id,
    sp.pool_address,
    se.first_stake_event_date
  --from nexusmutual_ethereum.staking_pools_list sp
  from query_5128062 sp -- staking pools list - base query
    inner join (
      select
        pool_id,
        cast(min(block_time) as date) as first_stake_event_date
      from query_3609519 -- staking events
      --from nexusmutual_ethereum.staking_events
      group by 1
    ) se on sp.pool_id = se.pool_id
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    d.timestamp as block_date
  from utils.days d
    inner join staking_pools sp on d.timestamp >=sp.first_stake_event_date
),

staked_nxm_per_pool as (
  select
    block_date,
    pool_id,
    pool_address,
    sum(coalesce(total_amount, 0)) as total_staked_nxm,
    dense_rank() over (partition by pool_id order by block_date desc) as pool_date_rn
  from (
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        left join query_3619534 se -- staking deposit extensions base query
        --left join nexusmutual_ethereum.staking_deposit_extensions se
          on d.pool_id = se.pool_id
         and d.block_date >= se.stake_start_date
         and d.block_date < se.stake_end_date
      group by 1, 2, 3
      union all
      -- withdrawals & burns
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        left join query_3609519 se -- staking events
        --left join nexusmutual_ethereum.staking_events se
          on d.pool_id = se.pool_id
         and d.block_date >= se.block_date
         and d.block_date < coalesce(se.tranche_expiry_date, current_date)
      where flow_type in ('withdraw', 'stake burn')
      group by 1, 2, 3
    ) t
  group by 1, 2, 3
)

select
  block_date,
  pool_id,
  pool_address,
  total_staked_nxm,
  pool_date_rn
from staked_nxm_per_pool
--where pool_date_rn = 1
--order by pool_id, block_date
