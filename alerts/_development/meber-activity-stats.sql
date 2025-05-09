with

member_activity as (
  select * from query_5097910 -- member activity base
),

member_activity_latest as (
  select
    max(block_time) as latest_block_time,
    max_by(active_members, block_time) as active_members_now,
    max_by(all_time_members, block_time) as all_time_members_now
  from member_activity
),

member_activity_historical as (
  select
    max(case when date_diff('day', block_time, latest_block_time) between 29 and 31 then active_members end) as active_members_30d_ago,
    max(case when date_diff('day', block_time, latest_block_time) between 89 and 91 then active_members end) as active_members_90d_ago,
    max(case when date_diff('day', block_time, latest_block_time) between 179 and 181 then active_members end) as active_members_180d_ago
  from member_activity, member_activity_latest
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
  from member_activity_latest l, member_activity_historical h
)

select * from member_activity_stats
