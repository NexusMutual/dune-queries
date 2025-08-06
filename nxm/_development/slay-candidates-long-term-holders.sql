with

top_holders as (
  select address, address_label, total_amount
  from query_5530985 -- nxm holdings
  where total_amount >= 5000
),

transfers as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'NXM' as token,
    evt_tx_from as tx_from,
    evt_tx_to as tx_to,
    "from" as transfer_from,
    "to" as transfer_to,
    value / 1e18 as amount,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.NXMToken_evt_Transfer
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'wNXM' as token,
    evt_tx_from as tx_from,
    evt_tx_to as tx_to,
    "from" as transfer_from,
    "to" as transfer_to,
    value / 1e18 as amount,
    evt_index,
    evt_tx_hash as tx_hash
  from wnxm_ethereum.wnxm_evt_transfer
),

movements as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    token,
    'in' as flow_type,
    transfer_to as address,
    th.address_label,
    amount,
    tx_hash
  from transfers t
    inner join top_holders th on t.transfer_to = th.address
  union all
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    token,
    'out' as flow_type,
    transfer_from as address,
    th.address_label,
    -1 * amount as amount,
    tx_hash
  from transfers t
    inner join top_holders th on t.transfer_from = th.address
),

movements_agg as (
  select
    block_date,
    address,
    address_label,
    sum(amount) as amount
  from movements
  group by 1, 2, 3
),

movements_agg_running as (
  select
    block_date,
    address,
    address_label,
    --coalesce(address_label, cast(address as varchar)) as address,
    sum(amount) over (partition by address order by block_date) as amount
  from movements_agg
),

movements_agg_running_with_next as (
  select
    block_date,
    address,
    address_label,
    amount,
    lead(block_date) over (partition by address order by block_date) as next_block_date
  from movements_agg_running
),

address_history_start_dates as (
  select
    address,
    min(block_date) as block_date_start
  from movements_agg_running_with_next
  group by 1
),

daily_sequence as (
  select
    s.address,
    d.timestamp as block_date
  from utils.days d
    inner join address_history_start_dates s on d.timestamp >= s.block_date_start
  where d.timestamp <= current_date
),

movements_forward_fill as (
  select
    d.block_date,
    ma.address,
    ma.amount
  from daily_sequence d
    left join movements_agg_running_with_next ma
      on d.block_date >= ma.block_date
      and (d.block_date < ma.next_block_date or ma.next_block_date is null)
      and d.address = ma.address
  where d.block_date <= current_date
),

stakers_stake_history as (
  select
    1 as version,
    block_date,
    --staker,
    staker_address as address,
    sum(amount) as amount
  from query_5584629 -- stakers stake history v1
  where staker_address in (select address from top_holders)
  group by 2, 3
  union all
  select
    2 as version,
    block_date,
    --staker,
    staker_address as address,
    sum(staked_nxm) as amount
  --from query_5578974 -- stakers stake history
  from dune.nexus_mutual.result_stakers_stake_history_base
  where staker_address in (select address from top_holders)
  group by 2, 3
),

movements_combined as (
  select
    block_date,
    address,
    sum(amount) as amount
  from (
    select
      block_date,
      address,
      amount
    from movements_forward_fill
    union all
    select
      block_date,
      address,
      amount
    from stakers_stake_history
  ) t
  group by 1, 2
),

totals_2_years_ago as (
  select
    block_date,
    address,
    amount
  from movements_combined
  where block_date = date_add('year', -2, current_date)
),

candidates as (
  select
    h.address,
    h.total_amount as current_balance,
    t.amount as balance_2_years_ago,
    case when t.amount >= h.total_amount * 0.5 then 'Y' else 'N' end as long_term_holder,
    case when t.amount >= h.total_amount * 0.5 then h.total_amount * 2 else h.total_amount end as adjusted_balance
  from top_holders h
    left join totals_2_years_ago t on h.address = t.address
)

select
  address,
  current_balance,
  balance_2_years_ago,
  long_term_holder,
  adjusted_balance,
  (adjusted_balance - min_ab) / nullif((max_ab - min_ab), 0) * 0.5 + 0.5 as adjusted_balance_ratio
from (
  select *,
    min(adjusted_balance) over() as min_ab,
    max(adjusted_balance) over() as max_ab
  from candidates
) t
order by adjusted_balance desc
