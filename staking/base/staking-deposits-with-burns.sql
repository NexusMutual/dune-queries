with recursive

-- base sparse series (no burns)
sd (block_time, pool_id, token_id, tranche_id, active_amount, stake_start_date, stake_end_date, stake_expiry_date) as (
  select
    block_time,
    pool_id,
    token_id,
    tranche_id,
    active_amount,
    stake_start_date,
    stake_end_date,
    stake_expiry_date
  from dune.nexus_mutual.result_staking_deposits
),

-- deposit-extension edges
ext (pool_id, token_id, block_time, evt_index, init_tranche_id, new_tranche_id, stake_expiry_date) as (
  select
    pool_id,
    token_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    cast(init_tranche_id as bigint) as init_tranche_id,
    cast(new_tranche_id as bigint) as new_tranche_id,
    cast(tranche_expiry_date as date) as stake_expiry_date
  from nexusmutual_ethereum.staking_events
  where flow_type = 'deposit extended'
    and init_tranche_id is not null
    and new_tranche_id is not null
),

-- stake burn events (pool-wide)
burns (pool_id, block_time, evt_index, burn_amount) as (
  select
    pool_id,
    block_time,
    cast(evt_index as bigint) as evt_index,
    amount as burn_amount
  from nexusmutual_ethereum.staking_events
  where flow_type = 'stake burn'
),

-- withdrawals to adjust token balance as-of burn time
withdraws (pool_id, token_id, tranche_id, block_time, evt_index, withdraw_amount) as (
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

-- tokens active in the pool at burn time (ignore burn.tranche_id), with as-of balance and window start
burn_active_tokens (pool_id, block_time, evt_index, token_id, tranche_id, token_amt_at_burn, token_expiry, window_block_time) as (
  select
    b.pool_id,
    b.block_time,
    b.evt_index,
    s.token_id,
    s.tranche_id,
    s.active_amount
      + coalesce((
          select sum(w.withdraw_amount)
          from withdraws w
          where w.pool_id = s.pool_id
            and w.token_id = s.token_id
            and w.tranche_id = s.tranche_id
            and (w.block_time > s.block_time or w.block_time = s.block_time)
            and (w.block_time < b.block_time or (w.block_time = b.block_time and w.evt_index < b.evt_index))
        ), 0) as token_amt_at_burn,
    s.stake_expiry_date as token_expiry,
    s.block_time as window_block_time
  from burns b
  join sd s
    on s.pool_id = b.pool_id
   and date(b.block_time) >= s.stake_start_date
   and date(b.block_time) < s.stake_end_date
   and s.block_time <= b.block_time
  where not exists (
    select 1
    from ext e0
    where e0.pool_id = s.pool_id
      and e0.token_id = s.token_id
      and e0.init_tranche_id = s.tranche_id
      and e0.block_time = b.block_time
      and e0.evt_index < b.evt_index
  )
),

-- pro-rata allocation per burn across all active token+tranche windows in that pool
burn_alloc (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, alloc_amount, origin_block_time) as (
  select
    t.pool_id,
    t.token_id,
    t.tranche_id,
    t.block_time,
    t.evt_index,
    t.token_expiry as stake_expiry_date,
    case
      when sum(greatest(t.token_amt_at_burn, 0)) over (partition by t.pool_id, t.block_time, t.evt_index) > 0
        then greatest(t.token_amt_at_burn, 0)
             / sum(greatest(t.token_amt_at_burn, 0)) over (partition by t.pool_id, t.block_time, t.evt_index)
             * (select b.burn_amount
                from burns b
                where b.pool_id = t.pool_id
                  and b.block_time = t.block_time
                  and b.evt_index = t.evt_index)
      else 0
    end as alloc_amount,
    t.window_block_time as origin_block_time
  from burn_active_tokens t
),

-- propagate burn allocations along earliest-next extension chain
burn_flow (pool_id, token_id, tranche_id, block_time, evt_index, stake_expiry_date, amount, origin_block_time) as (
  select
    a.pool_id,
    a.token_id,
    a.tranche_id,
    a.block_time,
    a.evt_index,
    a.stake_expiry_date,
    a.alloc_amount as amount,
    a.origin_block_time
  from burn_alloc a

  union all

  select
    e.pool_id,
    e.token_id,
    e.new_tranche_id as tranche_id,
    e.block_time,
    e.evt_index,
    e.stake_expiry_date,
    f.amount,
    f.origin_block_time
  from burn_flow f
  join ext e
    on e.pool_id = f.pool_id
   and e.token_id = f.token_id
   and e.init_tranche_id = f.tranche_id
   and (f.block_time < e.block_time or (f.block_time = e.block_time and f.evt_index < e.evt_index))
   and f.stake_expiry_date > date(e.block_time)
  where not exists (
    select 1
    from ext e2
    where e2.pool_id = e.pool_id
      and e2.token_id = e.token_id
      and e2.init_tranche_id = e.init_tranche_id
      and (f.block_time < e2.block_time or (f.block_time = e2.block_time and f.evt_index < e2.evt_index))
      and (e2.block_time < e.block_time or (e2.block_time = e.block_time and e2.evt_index < e.evt_index))
  )
),

-- deltas that land on extension timestamps
ext_burn_adjust (block_time, pool_id, token_id, tranche_id, burn_delta) as (
  select
    f.block_time,
    f.pool_id,
    f.token_id,
    f.tranche_id,
    sum(f.amount) as burn_delta
  from burn_flow f
  join ext e
    on e.pool_id = f.pool_id
   and e.token_id = f.token_id
   and e.new_tranche_id = f.tranche_id
   and e.block_time = f.block_time
   and e.evt_index = f.evt_index
  group by 1, 2, 3, 4
),

-- deltas applied at the origin window when there is no next extension after the burn
origin_burn_adjust (block_time, pool_id, token_id, tranche_id, burn_delta) as (
  select
    a.origin_block_time as block_time,
    a.pool_id,
    a.token_id,
    a.tranche_id,
    sum(a.alloc_amount) as burn_delta
  from burn_alloc a
  where not exists (
    select 1
    from ext e
    where e.pool_id = a.pool_id
      and e.token_id = a.token_id
      and e.init_tranche_id = a.tranche_id
      and (a.block_time < e.block_time or (a.block_time = e.block_time and a.evt_index < e.evt_index))
      and a.stake_expiry_date > date(e.block_time)
  )
  group by 1, 2, 3, 4
),

-- aggregate all burn deltas per sd row key
burn_adjust (block_time, pool_id, token_id, tranche_id, burn_delta) as (
  select
    block_time,
    pool_id,
    token_id,
    tranche_id,
    sum(burn_delta) as burn_delta
  from (
    select block_time, pool_id, token_id, tranche_id, burn_delta from ext_burn_adjust
    union all
    select block_time, pool_id, token_id, tranche_id, burn_delta from origin_burn_adjust
  ) u
  group by 1, 2, 3, 4
)

select
  s.block_time,
  s.pool_id,
  s.token_id,
  s.tranche_id,
  s.active_amount + coalesce(a.burn_delta, 0) as active_amount,
  s.stake_start_date,
  s.stake_end_date,
  s.stake_expiry_date
from sd s
left join burn_adjust a
  on a.pool_id = s.pool_id
 and a.token_id = s.token_id
 and a.tranche_id = s.tranche_id
 and a.block_time = s.block_time
order by 1, 2, 3, 4
