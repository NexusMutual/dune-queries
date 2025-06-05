with

member_activity as (
  select
    block_time,
    block_date,
    member,
    is_active,  
    active_members,
    all_time_members
  from query_5097910 -- member whitelist - base
),

member_activity_latest as (
  select
    min(block_date) as earliest_block_date,
    max(block_date) as latest_block_date,
    max_by(active_members, block_time) as active_members_now,
    max_by(all_time_members, block_time) as all_time_members_now
  from member_activity
),

member_activity_daily as (
  select
    block_date,
    max_by(active_members, block_time) as active_members,
    max_by(all_time_members, block_time) as all_time_members
  from member_activity
  group by 1
),

member_activity_daily_with_next as (
  select
    *,
    lead(block_date) over (order by block_date) as next_update_date
  from member_activity_daily
),

member_activity_forward_fill as (
  select
    d.timestamp as block_date,
    dn.active_members,
    dn.all_time_members
  from utils.days d
    cross join member_activity_latest l
    left join member_activity_daily_with_next dn
      on d.timestamp >= dn.block_date
      and (d.timestamp < dn.next_update_date or dn.next_update_date is null)
  where d.timestamp >= l.earliest_block_date
    and d.timestamp <= current_date
),

member_activity_historical as (
  select
    max(case when date_diff('day', block_date, latest_block_date) between 29 and 31 then active_members end) as active_members_30d_ago,
    max(case when date_diff('day', block_date, latest_block_date) between 89 and 91 then active_members end) as active_members_90d_ago,
    max(case when date_diff('day', block_date, latest_block_date) between 179 and 181 then active_members end) as active_members_180d_ago
  from member_activity_forward_fill maf
    cross join member_activity_latest l
)

select
  l.latest_block_date as latest_member_activity_date,
  l.all_time_members_now as all_time_members,
  l.active_members_now as active_members,
  -- 30d
  h.active_members_30d_ago,
  l.active_members_now - h.active_members_30d_ago as active_members_30d_change,
  round(100.0 * (l.active_members_now - h.active_members_30d_ago) / nullif(h.active_members_30d_ago, 0), 2) as active_members_30d_pct_change,
  -- 90d
  h.active_members_90d_ago,
  l.active_members_now - h.active_members_90d_ago as active_members_90d_change,
  round(100.0 * (l.active_members_now - h.active_members_90d_ago) / nullif(h.active_members_90d_ago, 0), 2) as active_members_90d_pct_change,
  -- 180d
  h.active_members_180d_ago,
  l.active_members_now - h.active_members_180d_ago as active_members_180d_change,
  round(100.0 * (l.active_members_now - h.active_members_180d_ago) / nullif(h.active_members_180d_ago, 0), 2) as active_members_180d_pct_change
from member_activity_latest l, member_activity_historical h
