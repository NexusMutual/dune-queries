with

staking_pools as (
  select distinct
    sp.pool_id,
    sp.pool_address,
    se.first_stake_event_date
  --from query_3859935 -- staking pools base (fallback) query
  from nexusmutual_ethereum.staking_pools sp
    inner join (
      select
        pool_address,
        cast(min(block_time) as date) as first_stake_event_date
      from query_3609519 -- staking events
      --from nexusmutual_ethereum.staking_events
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

staked_nxm_per_pool_n_token as (
  select
    block_date,
    pool_id,
    pool_address,
    token_id,
    sum(coalesce(total_amount, 0)) as total_staked_nxm,
    max(stake_expiry_date) as stake_expiry_date,
    dense_rank() over (partition by pool_id, token_id order by block_date desc) as token_date_rn
  from (
      -- deposits & deposit extensions
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.token_id,
        sum(se.total_amount) as total_amount,
        max(se.tranche_expiry_date) as stake_expiry_date
      from staking_pool_day_sequence d
        left join query_3619534 se -- staking deposit extensions base query
        --left join nexusmutual_ethereum.staking_deposit_extensions se
          on d.pool_address = se.pool_address
         and d.block_date between date_trunc('day', se.block_time) and se.tranche_expiry_date
         and se.token_tranche_rn = 1
      group by 1, 2, 3, 4
      union all
      -- withdrawals (& burns?)
      select
        d.block_date,
        d.pool_id,
        d.pool_address,
        se.token_id,
        sum(se.amount) as total_amount,
        cast(null as date) as stake_expiry_date -- no point pulling stake_expiry_date for withdrawals
      from staking_pool_day_sequence d
        left join query_3609519 se -- staking events
        --left join nexusmutual_ethereum.staking_events se
          on d.pool_address = se.pool_address
         and d.block_date between date_trunc('day', se.block_time) and se.tranche_expiry_date
      where 1=1
        --and flow_type in ('withdraw', 'stake burn')
        and flow_type = 'withdraw' -- burn TBD
      group by 1, 2, 3, 4
    ) t
  group by 1, 2, 3, 4
)

select
  block_date,
  pool_id,
  pool_address,
  token_id,
  total_staked_nxm,
  stake_expiry_date,
  token_date_rn
from staked_nxm_per_pool_n_token
--where token_date_rn = 1
--order by pool_id, block_date
