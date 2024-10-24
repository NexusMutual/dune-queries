with

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    se.first_stake_event_date
  --from query_3859935 sp -- staking pools base (fallback) query
  from query_4167546 sp -- staking pools - spell de-duped
  --from nexusmutual_ethereum.staking_pools sp
    inner join (
      select
        pool_address,
        cast(min(block_time) as date) as first_stake_event_date
      --from query_3609519 -- staking events
      from nexusmutual_ethereum.staking_events
      group by 1
    ) se on sp.pool_address = se.pool_address
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.pool_address,
    s.block_date
  from staking_pools sp
    cross join unnest (
      sequence(
        cast(date_trunc('day', sp.first_stake_event_date) as timestamp),
        cast(date_trunc('day', now()) as timestamp),
        interval '1' day
      )
    ) as s(block_date)
),

staked_per_pool as (
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
        --left join query_3619534 se -- staking deposit extensions base query
        left join nexusmutual_ethereum.staking_deposit_extensions se
          on d.pool_address = se.pool_address
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
        --left join query_3609519 se -- staking events
        left join nexusmutual_ethereum.staking_events se
          on d.pool_address = se.pool_address
         and d.block_date >= se.block_date
         and d.block_date < se.tranche_expiry_date
      --where flow_type in ('withdraw', 'stake burn')
      where flow_type = 'withdraw'
      group by 1, 2, 3
    ) t
  group by 1, 2, 3
),

staked_per_pool_tranche as (
  select
    block_date,
    pool_id,
    pool_address,
    tranche_id,
    sum(coalesce(total_amount, 0)) as total_tranche_staked_nxm,
    dense_rank() over (partition by pool_id, tranche_id order by block_date desc) as pool_tranche_date_rn
  from (
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.current_tranche_id as tranche_id,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        --left join query_3619534 se -- staking deposit extensions base query
        left join nexusmutual_ethereum.staking_deposit_extensions se
          on d.pool_address = se.pool_address
         and d.block_date >= se.stake_start_date
         and d.block_date < se.stake_end_date
      group by 1, 2, 3, 4
      union all
      -- withdrawals & burns
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.tranche_id,
        sum(se.amount) as total_amount
      from staking_pool_day_sequence d
        --left join query_3609519 se -- staking events
        left join nexusmutual_ethereum.staking_events se
          on d.pool_address = se.pool_address
         and d.block_date >= se.block_date
         and d.block_date < se.tranche_expiry_date
      --where flow_type in ('withdraw', 'stake burn')
      where flow_type = 'withdraw'
      group by 1, 2, 3, 4
    ) t
  group by 1, 2, 3, 4
),

staked_nxm_combined as (
  select
    p.block_date,
    p.pool_id,
    p.pool_address,
    pt.tranche_id,
    p.total_staked_nxm,
    pt.total_tranche_staked_nxm,
    se.amount as burn_amount,
    se.amount * pt.total_tranche_staked_nxm / p.total_staked_nxm as burn_per_tranche,
    p.pool_date_rn,
    pt.pool_tranche_date_rn
  from staked_per_pool p
    inner join staked_per_pool_tranche pt on p.pool_id = pt.pool_id and p.block_date = pt.block_date
    inner join nexusmutual_ethereum.staking_events se on p.pool_address = se.pool_address and p.block_date = se.block_date
  where se.flow_type = 'stake burn'
)

select * from staked_nxm_combined where pool_id = 11 order by 1


/*
staked_nxm_combined as (
  select
    p.block_date,
    p.pool_id,
    p.pool_address,
    pt.tranche_id,
    p.total_staked_nxm,
    pt.total_tranche_staked_nxm,
    se.amount,
    pt.total_tranche_staked_nxm + coalesce(se.amount, 0) * pt.total_tranche_staked_nxm / p.total_staked_nxm as total_incl_burn,
    if(se.flow_type = 'stake burn', from_unixtime(91.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), se.block_date) / 91.0) + 1 as double))) as burn_tranche_expiry_date,
    p.pool_date_rn,
    pt.pool_tranche_date_rn
  from staked_per_pool p
    left join nexusmutual_ethereum.staking_events se on p.pool_address = se.pool_address and se.flow_type = 'stake burn'
      and p.block_date >= se.block_date and p.block_date <= current_date
    inner join staked_per_pool_tranche pt on p.pool_id = pt.pool_id
      and p.block_date = pt.block_date
      /*
      and ((se.flow_type <> 'stake burn' and p.block_date = pt.block_date)
        or (se.flow_type = 'stake burn'
            and p.block_date < from_unixtime(91.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), se.block_date) / 91.0) + 1 as double))
            and p.block_date = pt.block_date)
        or (se.flow_type = 'stake burn'
            and p.block_date >= from_unixtime(91.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), se.block_date) / 91.0) + 1 as double))
            and p.block_date >= from_unixtime(91.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), se.block_date) / 91.0) + 1 as double)))
      )
      */
),


test_step as (
  select
    block_date,
    pool_id,
    pool_address,
    sum(total_incl_burn) as total_staked_nxm,
    min(pool_date_rn) as pool_date_rn
  from staked_nxm_combined
  group by 1, 2, 3
)

select
  block_date,
  pool_id,
  pool_address,
  total_staked_nxm,
  pool_date_rn
from test_step
where pool_id = 11
  --and block_date = timestamp '2023-10-14'
order by block_date desc
*/
