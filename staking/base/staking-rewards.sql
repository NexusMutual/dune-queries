with

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    floor(date_diff('day', from_unixtime(0), cover_end_date) / 28) as cover_end_bucket_id,
    from_unixtime(28.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), cover_end_time) / 28) + 1 as double)) as cover_end_bucket_expiry_date,
    date_diff(
      'second',
      cover_start_time,
      from_unixtime(28.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), cover_end_time) / 28) + 1 as double)) -- cover_end_bucket_expiry_date (rathan than cover_end_time)
    ) as cover_period_seconds,
    staking_pool_id,
    product_id,
    block_number,
    trace_address,
    tx_hash
  from nexusmutual_ethereum.covers_v2
  --from query_3788370 -- covers v2 base (fallback) query
)

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
