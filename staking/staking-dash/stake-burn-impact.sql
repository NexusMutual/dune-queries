/*
tokens impacted by stake burns (pre vs post) using query_5651171
- pre: latest per-origin row strictly before burn, with stake_expiry_date > date(burn)
- post: synthetic rows at burn ts (origin_block_time is null) aggregated per token
*/

with

stakers_base as (
  select
    block_time,
    pool_id,
    token_id,
    staker,
    pool_token_rn
  from query_5578777 -- stakers - base
),

-- per-origin series incl burns (synthetic rows have origin_block_time is null)
sd as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    active_amount,
    stake_expiry_date,
    origin_tx_hash,
    origin_evt_index,
    origin_block_time,
    stake_start_date,
    stake_end_date
  from query_5651171 -- staking deposits with burns - base
),

-- stake burn events
burns as (
  select
    pool_id,
    block_time,
    evt_index
  from nexusmutual_ethereum.staking_events
  where flow_type = 'stake burn'
),

-- last origin row strictly before burn and not expired before that day
pre_origin as (
  select
    b.pool_id,
    b.block_time as burn_block_time,
    b.evt_index as burn_evt_index,
    s.token_id,
    s.origin_tx_hash,
    s.origin_evt_index,
    s.active_amount,
    row_number() over (
      partition by b.pool_id, b.block_time, b.evt_index, s.token_id, s.origin_tx_hash, s.origin_evt_index
      order by s.block_time desc, coalesce(s.evt_index, -1) desc
    ) as rn
  from burns b
    inner join sd s on s.pool_id = b.pool_id
      and s.origin_block_time is not null
      and (s.block_time < b.block_time or (s.block_time = b.block_time and coalesce(s.evt_index, -1) < b.evt_index))
      and s.stake_expiry_date > date(b.block_time) -- exclude origins already expired before burn day
),

-- token-level pre totals
token_pre as (
  select
    pool_id,
    burn_block_time,
    burn_evt_index,
    token_id,
    sum(active_amount) as pre_burn_active_stake
  from pre_origin
  where rn = 1
  group by 1, 2, 3, 4
),

-- token-level post totals at exact burn ts (synthetic rows)
token_post as (
  select
    b.pool_id,
    b.block_time as burn_block_time,
    b.evt_index as burn_evt_index,
    s.token_id,
    sum(s.active_amount) as post_burn_active_stake
  from burns b
    inner join sd s on s.pool_id = b.pool_id
      and s.block_time = b.block_time
      and coalesce(s.evt_index, -1) = b.evt_index
      and s.origin_block_time is null
  group by 1, 2, 3, 4
)

select
  p.burn_block_time,
  p.pool_id,
  p.token_id,
  p.pre_burn_active_stake,
  coalesce(po.post_burn_active_stake, 0) as post_burn_active_stake,
  coalesce(po.post_burn_active_stake, 0) - p.pre_burn_active_stake as burn_delta
from token_pre p
  left join token_post po on po.pool_id = p.pool_id
    and po.burn_block_time = p.burn_block_time
    and po.burn_evt_index = p.burn_evt_index
    and po.token_id = p.token_id
where p.pre_burn_active_stake > 0
  and p.pool_id = 22
order by 1, 2, 3
