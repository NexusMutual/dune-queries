with

staking_events as (
  select
    block_time,
    block_date,
    block_number,
    event_type,
    product_address,
    staker,
    amount,
    tx_hash
  from query_5579588 -- staking events v1 - base
  where event_type in ('deposit', 'withdraw', 'rewards')
),

stake_agg as (
  select
    block_date,
    staker,
    sum(amount) as amount
  from staking_events
  group by 1, 2
),

stake_running_total as (
  select
    block_date,
    staker,
    sum(amount) over (partition by staker order by block_date) as amount
  from stake_agg
),

stake_running_total_with_next as (
  select
    block_date,
    staker,
    amount,
    lead(block_date) over (partition by staker order by block_date) as next_block_date
  from stake_running_total
),

staker_history_start_dates as (
  select
    staker,
    min(block_date) as block_date_start
  from stake_running_total_with_next
  group by 1
),

daily_sequence as (
  select
    s.staker,
    d.timestamp as block_date
  from utils.days d
    inner join staker_history_start_dates s on d.timestamp >= s.block_date_start
  where d.timestamp <= timestamp '2023-03-08'
),

stake_forward_fill as (
  select
    d.block_date,
    d.staker,
    coalesce(sr.amount, 0) as amount
  from daily_sequence d
    left join stake_running_total_with_next sr
      on d.block_date >= sr.block_date
      and (d.block_date < sr.next_block_date or sr.next_block_date is null)
      and d.staker = sr.staker
  where d.block_date <= current_date
)

select
  sff.block_date,
  sff.staker as staker_address,
  ens.name as staker_ens,
  coalesce(ens.name, cast(sff.staker as varchar)) as staker,
  sff.amount
from stake_forward_fill sff
  left join labels.ens on sff.staker = ens.address
order by 1, 2
