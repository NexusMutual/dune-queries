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
)

select
  address,
  address_label,
  case
    when address_label is null
    then concat(substring(cast(address as varchar), 1, 6), '..', substring(cast(address as varchar), length(cast(address as varchar)) - 3, 4))
    else address_label
  end as address_label_formatted,
  initial_distribution
from tge_users
order by 4 desc
