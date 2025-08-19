with

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

ext as (
  select
    pool_id,
    token_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    cast(init_tranche_id as bigint) as init_tranche_id,
    cast(new_tranche_id as bigint) as new_tranche_id
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and init_tranche_id is not null
    and new_tranche_id is not null
),

burns as (
  select
    pool_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    amount as burn_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'stake burn'
),

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
          select sum(w.withdraw_amount)
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
             * (select b.burn_amount
                from burns b
                where b.pool_id = t.pool_id
                  and b.block_time = t.block_time
                  and b.evt_index = t.evt_index)
      else 0
    end as alloc_amount
  from burn_active_origins t
),

next_event_after_burn as (
  select
    a.pool_id,
    a.block_time as burn_block_time,
    a.evt_index as burn_evt_index,
    s.origin_tx_hash,
    s.origin_evt_index,
    s.block_time as target_block_time,
    s.evt_index as target_evt_index,
    s.tranche_id as target_tranche_id,
    row_number() over (
      partition by a.pool_id, a.block_time, a.evt_index, s.origin_tx_hash, s.origin_evt_index
      order by s.block_time, s.evt_index
    ) as rn
  from burn_alloc a
  join sd s
    on s.pool_id = a.pool_id
   and s.origin_tx_hash = a.origin_tx_hash
   and s.origin_evt_index = a.origin_evt_index
   and (
         s.block_time >  a.block_time
      or (s.block_time = a.block_time and s.evt_index > a.evt_index)
       )
),

-- CHANGED: use target_tranche_id so the delta lands on the correct (new) tranche row
burn_to_next as (
  select
    n.target_block_time as block_time,
    n.target_evt_index as evt_index,
    a.pool_id,
    a.token_id,
    n.target_tranche_id as tranche_id,
    a.origin_tx_hash,
    a.origin_evt_index,
    sum(a.alloc_amount) as burn_delta
  from burn_alloc a
  join next_event_after_burn n
    on n.pool_id = a.pool_id
   and n.burn_block_time = a.block_time
   and n.burn_evt_index = a.evt_index
   and n.origin_tx_hash = a.origin_tx_hash
   and n.origin_evt_index = a.origin_evt_index
   and n.rn = 1
  group by 1, 2, 3, 4, 5, 6, 7
),

holder_rows as (
  select
    a.pool_id,
    a.block_time as burn_block_time,
    a.evt_index as burn_evt_index,
    s.token_id,
    s.tranche_id,
    s.origin_tx_hash,
    s.origin_evt_index,
    s.active_amount as holder_active_amount,
    s.stake_expiry_date as holder_expiry
  from burn_alloc a
  join sd s
    on s.pool_id = a.pool_id
   and s.origin_tx_hash = a.origin_tx_hash
   and s.origin_evt_index = a.origin_evt_index
   and date(a.block_time) >= s.stake_start_date
   and date(a.block_time) < s.stake_end_date
   and (
         s.block_time <  a.block_time
      or (s.block_time = a.block_time and coalesce(s.evt_index, -1) < a.evt_index)
       )
  where not exists (
    select 1
    from next_event_after_burn n
    where n.pool_id = a.pool_id
      and n.burn_block_time = a.block_time
      and n.burn_evt_index = a.evt_index
      and n.origin_tx_hash = a.origin_tx_hash
      and n.origin_evt_index = a.origin_evt_index
      and n.rn = 1
  )
),

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
      + sum(a.alloc_amount) over (
          partition by h.pool_id, h.origin_tx_hash, h.origin_evt_index
          order by h.burn_block_time, h.burn_evt_index
          rows between unbounded preceding and current row
        ) as synthetic_active_amount,
    h.holder_expiry as stake_expiry_date
  from holder_rows h
  join burn_alloc a
    on a.pool_id = h.pool_id
   and a.block_time = h.burn_block_time
   and a.evt_index = h.burn_evt_index
   and a.origin_tx_hash = h.origin_tx_hash
   and a.origin_evt_index = h.origin_evt_index
),

burn_adjust as (
  select
    block_time,
    evt_index,
    pool_id,
    token_id,
    tranche_id,
    origin_tx_hash,
    origin_evt_index,
    sum(burn_delta) as burn_delta
  from burn_to_next
  group by 1, 2, 3, 4, 5, 6, 7
),

final_rows as (
  select
    s.block_time,
    s.evt_index,
    s.pool_id,
    s.token_id,
    s.tranche_id,
    s.active_amount + coalesce(b.burn_delta, 0) as active_amount,
    s.stake_start_date,
    s.stake_end_date,
    s.stake_expiry_date,
    s.origin_tx_hash,
    s.origin_evt_index,
    s.origin_block_time
  from sd s
  left join burn_adjust b
    on b.pool_id = s.pool_id
   and b.token_id = s.token_id
   and b.tranche_id = s.tranche_id
   and b.origin_tx_hash = s.origin_tx_hash
   and b.origin_evt_index = s.origin_evt_index
   and b.block_time = s.block_time
   and coalesce(b.evt_index, -1) = coalesce(s.evt_index, -1)

  union all

  select
    hb.block_time,
    hb.evt_index,
    hb.pool_id,
    hb.token_id,
    hb.tranche_id,
    hb.synthetic_active_amount as active_amount,
    date(hb.block_time) as stake_start_date,
    hb.stake_expiry_date as stake_end_date,
    hb.stake_expiry_date as stake_expiry_date,
    hb.origin_tx_hash,
    hb.origin_evt_index,
    null as origin_block_time
  from holder_burns hb
),

with_intervals as (
  -- recompute next event per origin (pool_id, token_id, origin_tx_hash, origin_evt_index)
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
