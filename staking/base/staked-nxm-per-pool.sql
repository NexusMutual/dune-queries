with

staking_pools as (
  select
    sp.pool_id,
    sp.pool_address,
    se.first_stake_event_date
  from nexusmutual_ethereum.staking_pools_list sp
  --from query_5128062 sp -- staking pools list - base query
    inner join (
      select
        pool_id,
        cast(min(block_time) as date) as first_stake_event_date
      from query_5734582 -- staking events - base root
      group by 1
    ) se on sp.pool_id = se.pool_id
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    d.timestamp as block_date
  from utils.days d
    inner join staking_pools sp on d.timestamp >= sp.first_stake_event_date
),

staked_nxm_per_pool as (
  select
    d.block_date,
    d.pool_id,
    d.pool_address,
    sum(se.active_amount) as total_staked_nxm
  from staking_pool_day_sequence d
    left join query_5651171 se -- staking deposits with burns - base
      on d.pool_id = se.pool_id
      and d.block_date >= se.stake_start_date
      and d.block_date < se.stake_end_date
  group by 1, 2, 3
),

staked_nxm_per_pool_final as (
  select
    block_date,
    pool_id,
    pool_address,
    total_staked_nxm,
    row_number() over (partition by pool_id order by block_date desc) as pool_date_rn
  from staked_nxm_per_pool
)

select
  block_date,
  pool_id,
  pool_address,
  total_staked_nxm,
  pool_date_rn
from staked_nxm_per_pool_final
--where pool_date_rn = 1
--order by pool_id, block_date
