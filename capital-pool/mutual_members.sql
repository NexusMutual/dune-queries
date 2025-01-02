with

memebership as (
  -- v1
  select
    date_trunc('day', call_block_time) as block_date,
    cardinality(userArray) as member_count
  from nexusmutual_ethereum.MemberRoles_call_addMembersBeforeLaunch
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  union all
  select
    date_trunc('day', call_block_time) as block_date,
    count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_kycVerdict
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
    and verdict
  group by 1
  union all
  -- v2
  select
    date_trunc('day', call_block_time) as block_date,
    count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_join
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  group by 1
  union all
    select
    date_trunc('day', call_block_time) as block_date,
    -1 * count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_withdrawMembership
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  group by 1
)

select
  block_date,
  sum(member_count) over (order by block_date) as running_member_count
from memebership
where block_date >= cast('{{Start Date}}' as timestamp)
  and block_date <= cast('{{End Date}}' as timestamp)
order by 1 desc
