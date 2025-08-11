with

address_labels as (
  select address, address_label from query_5534312
),

tge_users as (
  select
    t.address,
    coalesce(al.address_label, ens.name) as address_label,
    t.amount / 1e18 as initial_distribution
  from nexusmutual_ethereum.MemberRoles_call_addMembersBeforeLaunch mbl
    cross join unnest(mbl.userArray, mbl.tokens) as t(address, amount)
    left join address_labels al on t.address = al.address
    left join labels.ens on t.address = ens.address
),

nxm_combined_history as (
  select
    h.block_date,
    coalesce(tu.address_label, cast(h.address as varchar)) as address,
    sum(h.amount) as amount
  from query_5616437 h -- nxm combined history - base
    inner join tge_users tu on h.address = tu.address
  group by 1, 2
)

select
  block_date,
  case
    when starts_with(address, '0x')
    then concat(substring(address, 1, 6), '..', substring(address, length(address) - 3, 4))
    else address
  end as address,
  amount
from nxm_combined_history
order by 1, 2
