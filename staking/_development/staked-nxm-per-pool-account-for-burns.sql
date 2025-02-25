with

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    se.first_stake_event_date
  --from query_3859935 sp -- staking pools base (fallback) query
  --from nexusmutual_ethereum.staking_pools sp
  from query_4167546 sp -- staking pools - spell de-duped
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

tranches as (
  select distinct
    coalesce(new_tranche_id, tranche_id) as tranche_id,
    tranche_expiry_date
  --from query_3609519 -- staking events
  from nexusmutual_ethereum.staking_events
),

deposits as (
  -- deposits & deposit extensions
  select
    d.block_date,
    d.pool_id,
    d.pool_address,
    --se.token_id,
    --se.init_tranche_id,
    se.current_tranche_id as tranche_id,
    sum(se.amount) as total_amount
  from staking_pool_day_sequence d
    --left join query_3619534 se -- staking deposit extensions base query
    left join nexusmutual_ethereum.staking_deposit_extensions se
      on d.pool_address = se.pool_address
      and d.block_date >= se.stake_start_date
      and d.block_date < se.stake_end_date
  group by 1, 2, 3, 4
),

burns_init as (
  select
    block_date,
    pool_id,
    pool_address,
    tranche_id,
    burn_total * staked_nxm / total_staked_nxm as burn_amount,
    staked_nxm + burn_total * staked_nxm / total_staked_nxm as new_total,
    cast(from_unixtime(91.0 * 86400.0 * cast(tranche_id + 1 as double)) as date) as tranche_expiry_date
  from (
    select
      se.block_date,
      d.pool_id,
      se.pool_address,
      d.tranche_id,
      se.amount as burn_total,
      d.staked_nxm,
      sum(d.staked_nxm) over (partition by se.pool_address, se.tranche_id) as total_staked_nxm
    from nexusmutual_ethereum.staking_events se
      --cross join tranches t
      inner join (
        select
          block_date,
          pool_id,
          pool_address,
          tranche_id,
          sum(coalesce(total_amount, 0)) as staked_nxm
        from deposits
        group by 1, 2, 3, 4
      ) d on se.block_date = d.block_date and se.pool_address = d.pool_address and d.tranche_id = d.tranche_id
    where se.flow_type = 'stake burn'
  ) t
),

burns as (
  select
    d.block_date,
    d.pool_id,
    d.pool_address,
    bi.tranche_id,
    sum(bi.burn_amount) as total_amount
  from staking_pool_day_sequence d
    left join burns_init bi
      on d.pool_address = bi.pool_address
      and d.block_date >= bi.block_date
      --and d.block_date < bi.tranche_expiry_date
  group by 1, 2, 3, 4
),

staked_nxm_per_pool as (
  select
    block_date,
    pool_id,
    pool_address,
    sum(coalesce(total_amount, 0)) as total_staked_nxm,
    dense_rank() over (partition by pool_id order by block_date desc) as pool_date_rn
  from (
      select
        block_date,
        pool_id,
        pool_address,
        tranche_id,
        total_amount
      from deposits
      union all
      select
        block_date,
        pool_id,
        pool_address,
        tranche_id,
        total_amount
      from burns
    ) t
  group by 1, 2, 3
)

select
  min(block_date) as block_date,
  total_staked_nxm
from staked_nxm_per_pool
where pool_id = 11
group by 2
order by 1
