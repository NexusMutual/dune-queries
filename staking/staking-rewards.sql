with

staking_pools as (
  select distinct
    pool_id,
    pool_address,
    pool_created_time
  --from query_3859935 -- staking pools base (fallback) query
  from nexusmutual_ethereum.staking_pools
),

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    date_diff('second', cover_start_time, cover_end_time) as cover_period_seconds,
    staking_pool_id,
    product_id,
    block_number,
    trace_address,
    tx_hash
  from nexusmutual_ethereum.covers_v2
  --from query_3788370 -- covers v2 base (fallback) query
),

staking_rewards as (
  select
    mr.call_block_time as block_time,
    date_trunc('day', mr.call_block_time) as block_date,
    mr.poolId as pool_id,
    c.product_id,
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    mr.amount / 1e18 as reward_amount_expected_total,
    mr.amount / c.cover_period_seconds / 1e18 as reward_amount_per_second,
    mr.amount / c.cover_period_seconds * 86400.0 / 1e18 as reward_amount_per_day,
    mr.call_tx_hash as tx_hash
  from (
      select call_block_time, call_block_number, poolId, amount, call_trace_address, call_tx_hash
      from nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards
      where call_success
      union all
      select call_block_time, call_block_number, poolId, amount, call_trace_address, call_tx_hash
      from nexusmutual_ethereum.TokenController2_call_mintStakingPoolNXMRewards
      where call_success
      union all
      select call_block_time, call_block_number, poolId, amount, call_trace_address, call_tx_hash
      from nexusmutual_ethereum.TokenController3_call_mintStakingPoolNXMRewards
      where call_success
    ) mr
    inner join covers c on mr.call_tx_hash = c.tx_hash and mr.call_block_number = c.block_number
  where mr.poolId = c.staking_pool_id
    and (c.trace_address is null
      or slice(mr.call_trace_address, 1, cardinality(c.trace_address)) = c.trace_address)
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

daily_rewards as (
  select distinct
    d.pool_id,
    d.block_date,
    sum(r.reward_amount_per_day) over (partition by d.pool_id order by d.block_date) as reward_amount_current_total,
    dense_rank() over (partition by d.pool_id order by d.block_date desc) as rn
  from staking_pool_day_sequence d
    left join staking_rewards r on d.pool_id = r.pool_id and d.block_date between date_add('day', 1, r.cover_start_date) and r.cover_end_date
),

expected_rewards as (
  select
    pool_id,
    sum(reward_amount_expected_total) as reward_amount_expected_total
  from staking_rewards
  group by 1
)

select *
from daily_rewards
order by pool_id, block_date
