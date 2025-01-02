with

nxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    'out' as transfer_type,
    "from" as address,
    -1 * (value / 1e18) as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
  --where value > 0
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    'in' as transfer_type,
    "to" as address,
    value / 1e18 as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
  --where value > 0
),

nxm_address_running_total as (
  select distinct
    block_date,
    address,
    sum(amount) over (partition by address order by block_date) as total_amount
  from nxm_transfer
  --where address <> 0x0000000000000000000000000000000000000000
),

exit_entrance as (
  select
    block_date,
    address,
    total_amount,
    -- total 1e-12 = "0" assumption
    case when total_amount < 1e-12 then -1 else 0 end as exited_mututal,
    case when total_amount >= 1e-12 then 1 else 0 end as in_mututal
  from nxm_address_running_total
),

unqiue_addresses as (
  select
    block_date,
    address,
    total_amount,
    exited_mututal,
    in_mututal,
    case
      when exited_mututal - coalesce(
        lag(exited_mututal) over (partition by address order by block_date),
        -1
      ) = -1 then -1
    end as exited, -- find first exit for the address
    case
      when in_mututal - coalesce(
        lag(in_mututal) over (partition by address order by block_date),
        0
      ) = 1 then 1
    end as entered -- find first enter for the address
  from exit_entrance
),

entered_and_exited as (
  select distinct
    block_date,
    -- running total for the firsts
    coalesce(sum(exited) over (partition by block_date), 0) as exited_per_day,
    coalesce(sum(entered) over (partition by block_date), 0) as entered_per_day
  from unqiue_addresses
),

nxm_holders as (
  select
    block_date,
    exited_per_day,
    entered_per_day,
    entered_per_day + exited_per_day as net_change,
    sum(entered_per_day + exited_per_day) over (order by block_date) as running_unique_users
  from entered_and_exited
)

select
  block_date,
  exited_per_day,
  entered_per_day,
  net_change,
  running_unique_users
from nxm_holders
where block_date >= cast('{{Start Date}}' as timestamp)
  and block_date <= cast('{{End Date}}' as timestamp)
order by 1 desc
