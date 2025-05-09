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
    count(distinct member) over (order by block_time, member) as all_time_members
  from whitelist
),

latest as (
  select
    max(block_time) as latest_block_time,
    max_by(active_members, block_time) as active_members_now,
    max_by(all_time_members, block_time) as all_time_members_now
  from members
),

historical as (
  select
    max(case when date_diff('day', block_time, latest_block_time) between 29 and 31 then active_members end) as active_members_30d_ago,
    max(case when date_diff('day', block_time, latest_block_time) between 89 and 91 then active_members end) as active_members_90d_ago,
    max(case when date_diff('day', block_time, latest_block_time) between 179 and 181 then active_members end) as active_members_180d_ago
  from members, latest
),

member_activity_stats as (
  select
    l.latest_block_time as latest_member_activity_time,
    l.all_time_members_now as all_time_members,
    l.active_members_now as active_members,
    -- 30d
    h.active_members_30d_ago,
    l.active_members_now - h.active_members_30d_ago as change_30d,
    round(100.0 * (l.active_members_now - h.active_members_30d_ago) / nullif(h.active_members_30d_ago, 0), 2) as pct_change_30d,
    -- 90d
    h.active_members_90d_ago,
    l.active_members_now - h.active_members_90d_ago as change_90d,
    round(100.0 * (l.active_members_now - h.active_members_90d_ago) / nullif(h.active_members_90d_ago, 0), 2) as pct_change_90d,
    -- 180d
    h.active_members_180d_ago,
    l.active_members_now - h.active_members_180d_ago as change_180d,
    round(100.0 * (l.active_members_now - h.active_members_180d_ago) / nullif(h.active_members_180d_ago, 0), 2) as pct_change_180d
  from latest l, historical h
)

--select distinct member from members -- list of all the members

select * from member_activity_stats
