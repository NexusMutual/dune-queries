with

whitelist_raw as (
  -- whitelist addition
  select
    call_block_time as block_time,
    _member as member,
    1 as counter,
    true as is_active
  from nexusmutual_ethereum.tokencontroller_call_addtowhitelist
  where call_success
  union all
  -- whitelist removal
  select
    call_block_time as block_time,
    _member as member,
    -1 as counter,
    false as is_active
  from nexusmutual_ethereum.tokencontroller_call_removefromwhitelist
  where call_success
  union all
  -- membership switch - previous member
  select
    evt_block_time as block_time,
    previousMember as member,
    -1 as counter,
    false as is_active
  from nexusmutual_ethereum.memberroles_evt_switchedmembership
  union all
  -- membership switch - new member
  select
    evt_block_time as block_time,
    newMember as member,
    1 as counter,
    true as is_active
  from nexusmutual_ethereum.memberroles_evt_switchedmembership
),

whitelist as (
  select distinct
    block_time,
    member,
    counter,
    is_active
  from whitelist_raw
),

members as (
  select
    block_time,
    member,
    counter,
    is_active,
    sum(counter) over (order by block_time, member) as active_members,
    count(distinct member) over (order by block_time, member) as all_time_members,
    row_number() over (partition by member order by block_time desc) as rn
  from whitelist
)

select
  block_time,
  member,
  is_active,
  active_members,
  all_time_members
from members
where rn = 1
--order by 1 desc
