/*
final per-origin series with pool-wide stake burns applied
- inputs:
  * dune.nexus_mutual.result_staking_origins_chain (sparse per-origin deposits/extensions, no burns)
  * nexusmutual_ethereum.staking_events (stake burn, withdraw for as-of exposure)
- method:
  * find all origins active at each burn (pool-wide, ignore burn.tranche_id)
  * split each burn pro-rata by as-of active amount (withdraw-adjusted)
  * emit burn markers at burn ts (to break windows) and take cumulative burns per origin
  * apply cumulative burns to all subsequent origin rows; add synthetic rows at burn ts
  * recompute stake windows so burns shorten intervals
*/

with

-- per-origin sparse chain
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
  from dune.nexus_mutual.result_staking_origins_chain
),

-- pool-wide burns
burns as (
  select
    pool_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    amount as burn_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'stake burn'
),

-- withdraws to compute as-of balances for pro-rata split
withdraws as (
  select
    pool_id,
    token_id,
    cast(tranche_id as bigint) as tranche_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    amount as withdraw_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'withdraw'
    and tranche_id is not null
),

-- origins active at the burn moment (window contains burn date)
burn_active_origins as (
  select
    b.pool_id,
    b.block_time,
    b.evt_index,
    s.token_id,
    s.tranche_id,
    s.origin_tx_hash,
    s.origin_evt_index,
    s.origin_block_time,
    s.active_amount
      + coalesce((
          select
            sum(w.withdraw_amount)
          from withdraws w
          where w.pool_id = s.pool_id
            and w.token_id = s.token_id
            and w.tranche_id = s.tranche_id
            and (w.block_time > s.block_time or (w.block_time = s.block_time and w.evt_index >= coalesce(s.evt_index, -1)))
            and (w.block_time < b.block_time or (w.block_time = b.block_time and w.evt_index < b.evt_index))
        ), 0) as origin_amt_at_burn
  from burns b
  join sd s
    on s.pool_id = b.pool_id
   and date(b.block_time) >= s.stake_start_date
   and date(b.block_time) < s.stake_end_date
   and s.block_time <= b.block_time
),

-- split each burn pro-rata across its active origins
burn_alloc as (
  select
    t.pool_id,
    t.block_time,
    t.evt_index,
    t.token_id,
    t.tranche_id,
    t.origin_tx_hash,
    t.origin_evt_index,
    t.origin_block_time,
    case
      when sum(greatest(t.origin_amt_at_burn, 0)) over (partition by t.pool_id, t.block_time, t.evt_index) > 0
        then greatest(t.origin_amt_at_burn, 0)
             / sum(greatest(t.origin_amt_at_burn, 0)) over (partition by t.pool_id, t.block_time, t.evt_index)
             * (select burn_amount from burns b where b.pool_id = t.pool_id and b.block_time = t.block_time and b.evt_index = t.evt_index)
      else 0
    end as alloc_amount
  from burn_active_origins t
),

-- burn markers at the burn ts per origin (to break windows + drive cumulative)
burn_markers as (
  select
    a.block_time,
    a.evt_index,
    a.pool_id,
    a.token_id,
    a.tranche_id,
    a.origin_tx_hash,
    a.origin_evt_index,
    a.origin_block_time,
    a.alloc_amount as burn_delta
  from burn_alloc a
),

-- last sd row strictly before each burn (to seed synthetic amounts)
holder_rows as (
  select
    x.pool_id,
    x.block_time as burn_block_time,
    x.evt_index as burn_evt_index,
    s.token_id,
    s.tranche_id,
    s.origin_tx_hash,
    s.origin_evt_index,
    s.active_amount as holder_active_amount,
    s.stake_expiry_date as holder_expiry,
    row_number() over (
      partition by x.pool_id, x.block_time, x.evt_index, s.token_id, s.origin_tx_hash, s.origin_evt_index
      order by s.block_time desc, coalesce(s.evt_index, -1) desc
    ) as rn
  from burn_markers x
  join sd s
    on s.pool_id = x.pool_id
   and s.token_id = x.token_id
   and s.origin_tx_hash = x.origin_tx_hash
   and s.origin_evt_index = x.origin_evt_index
   and (s.block_time < x.block_time or (s.block_time = x.block_time and coalesce(s.evt_index, -1) < x.evt_index))
),

-- synthetic rows at burn ts with cumulative burn within holder window
holder_burns as (
  select
    h.pool_id,
    h.token_id,
    h.tranche_id,
    h.origin_tx_hash,
    h.origin_evt_index,
    h.burn_block_time as block_time,
    h.burn_evt_index as evt_index,
    h.holder_active_amount
      + sum(m.burn_delta) over (
          partition by h.pool_id, h.token_id, h.origin_tx_hash, h.origin_evt_index
          order by h.burn_block_time, h.burn_evt_index
          rows between unbounded preceding and current row
        ) as synthetic_active_amount,
    h.holder_expiry as stake_expiry_date
  from holder_rows h
  join burn_markers m
    on m.pool_id = h.pool_id
   and m.block_time = h.burn_block_time
   and m.evt_index = h.burn_evt_index
   and m.token_id = h.token_id
   and m.origin_tx_hash = h.origin_tx_hash
   and m.origin_evt_index = h.origin_evt_index
  where h.rn = 1
),

-- sd rows (zero delta) + burn markers (negative delta) for cumulative application
rows_and_burns as (
  select
    s.block_time,
    s.evt_index,
    s.pool_id,
    s.token_id,
    s.tranche_id,
    s.active_amount,
    s.stake_expiry_date,
    s.origin_tx_hash,
    s.origin_evt_index,
    s.origin_block_time,
    cast(0 as double) as burn_delta,
    true as is_sd_row
  from sd s
  union all
  select
    m.block_time,
    m.evt_index,
    m.pool_id,
    m.token_id,
    null as tranche_id,
    null as active_amount,
    null as stake_expiry_date,
    m.origin_tx_hash,
    m.origin_evt_index,
    null as origin_block_time,
    m.burn_delta,
    false as is_sd_row
  from burn_markers m
),

-- cumulative burn per origin over time
rows_with_cum as (
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
    sum(burn_delta) over (
      partition by pool_id, token_id, origin_tx_hash, origin_evt_index
      order by block_time, coalesce(evt_index, -1)
      rows between unbounded preceding and current row
    ) as cum_burn_delta,
    is_sd_row
  from rows_and_burns
),

-- numeric series: sd rows + cumulative burns + synthetic burn rows
final_rows as (
  select
    r.block_time,
    r.evt_index,
    r.pool_id,
    r.token_id,
    r.tranche_id,
    r.active_amount + r.cum_burn_delta as active_amount,
    r.stake_expiry_date,
    r.origin_tx_hash,
    r.origin_evt_index,
    r.origin_block_time
  from rows_with_cum r
  where r.is_sd_row
  union all
  select
    hb.block_time,
    hb.evt_index,
    hb.pool_id,
    hb.token_id,
    hb.tranche_id,
    hb.synthetic_active_amount as active_amount,
    hb.stake_expiry_date,
    hb.origin_tx_hash,
    hb.origin_evt_index,
    null as origin_block_time
  from holder_burns hb
),

-- recompute stake windows so burns split intervals
with_intervals as (
  select
    fr.block_time,
    fr.evt_index,
    fr.pool_id,
    fr.token_id,
    fr.tranche_id,
    fr.active_amount,
    fr.stake_expiry_date,
    fr.origin_tx_hash,
    fr.origin_evt_index,
    fr.origin_block_time,
    lead(fr.block_time) over (
      partition by fr.pool_id, fr.token_id, fr.origin_tx_hash, fr.origin_evt_index
      order by fr.block_time, coalesce(fr.evt_index, -1)
    ) as next_block_time
  from final_rows fr
)

-- final output
select
  block_time,
  pool_id,
  token_id,
  tranche_id,
  active_amount,
  date(block_time) as stake_start_date,
  coalesce(date(next_block_time), stake_expiry_date) as stake_end_date,
  stake_expiry_date,
  origin_tx_hash,
  origin_evt_index,
  origin_block_time
from with_intervals
order by 1, 2, 3, 4, 9, 10
