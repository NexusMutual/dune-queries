with

nxm_initial_allocation as (
  select
    t.user,
    t.amount / 1e18 as nxm_amount
  from nexusmutual_ethereum.MemberRoles_call_addMembersBeforeLaunch
    cross join unnest(tokens, userArray) as t(amount, user)
),

nxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    'out' as transfer_type,
    "from" as address,
    -1 * (value / 1e18) as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    'in' as transfer_type,
    "to" as address,
    value / 1e18 as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
),

nxm_holders as (
  select
    address,
    sum(amount) as amount
  from nxm_transfer
  group by 1
  having sum(amount) > 1e-11 -- assumed "0"
),

swaps_below as (
  select
    member,
    count(*) as swap_count,
    sum(nxm_in) as nxm_in,
    sum(eth_out) as eth_out,
    min(block_date) as first_swap_date,
    max(block_date) as last_swap_date
  from query_5232729 -- RAMM swaps below per member - base
  group by 1
)

select
  s.member,
  ens.name as member_ens,
  coalesce(h.amount, 0) as nxm_amount_held,
  i.nxm_amount as nxm_initial_allocation,
  s.swap_count,
  s.nxm_in as nxm_swapped,
  s.eth_out as eth_swapped,
  s.first_swap_date,
  s.last_swap_date
from swaps_below s
  left join nxm_holders h on s.member = h.address
  left join nxm_initial_allocation i on s.member = i.user
  left join labels.ens on s.member = ens.address
order by 4 desc
