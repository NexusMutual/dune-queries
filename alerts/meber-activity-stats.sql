with

items (id, item_1, item_2) as (
  values
    -- MEMBERS SECTION
    (1, '🐢 members', null),
    --(2, 'latest activity date', null),
    (3, 'all time members', 'active members'),
    (4, '30d change', '30d % change'),
    (5, '90d change', '90d % change'),
    (6, '180d change', '180d % change'),
    (7, '--------------------------------', '--------------------------------'),
    -- ADDITIONAL SECTION
    (8, '🐢 protocol engagement', null),
    (9, 'all time NXM holders', 'current NXM holders'),
    (10, 'all time buyers', 'active cover buyers'),
    (11, 'all time stakers', 'current stakers')
),

member_whitelist as (
  select
    block_time,
    block_date,
    member,
    is_active,
    active_members,
    all_time_members
  from query_5097910 -- member whitelist - base
),

member_activity_stats as (
  select
    latest_member_activity_date,
    all_time_members,
    active_members,
    -- 30d
    active_members_30d_ago,
    active_members_30d_change,
    active_members_30d_pct_change,
    -- 90d
    active_members_90d_ago,
    active_members_90d_change,
    active_members_90d_pct_change,
    -- 180d
    active_members_180d_ago,
    active_members_180d_change,
    active_members_180d_pct_change
  from query_5239687 -- member activity - base
),

buyers as (
  select distinct
    version,
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_owner,
    if(cover_end_time >= now(), true, false) as is_active
  --from query_5119916 -- covers full list - base
  from nexusmutual_ethereum.covers_full_list
  where is_migrated = false
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
  --having sum(amount) > 1e-11 -- assumed "0"
),

staking_mints as (
  select
    call_block_time as block_time,
    poolId as pool_id,
    output_id as token_id,
    "to" as to_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingNFT_call_mint
  where call_success
),

staking_transfers as (
  select
    evt_block_time as block_time,
    tokenId as token_id,
    "from" as from_address,
    "to" as to_address,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingNFT_evt_Transfer
),

stakers as (
  select
    block_time,
    pool_id,
    token_id,
    staker,
    tx_hash,
    row_number() over (partition by pool_id, token_id order by block_time desc, evt_index desc) as pool_token_rn
  from (
    select
      block_time,
      pool_id,
      token_id,
      to_address as staker,
      cast(-1 as bigint) as evt_index,
      tx_hash
    from staking_mints
    union all
    select
      t.block_time,
      m.pool_id,
      m.token_id,
      t.to_address as staker,
      t.evt_index,
      t.tx_hash
    from staking_mints m
      inner join staking_transfers t on m.token_id = t.token_id
    where t.from_address <> 0x0000000000000000000000000000000000000000 -- excl mints
  ) t
),

stakers_active_stake as (
  select
    s.staker,
    s.pool_id,
    s.token_id,
    s.pool_token_rn,
    st.total_staked_nxm as nxm_active_stake
  from stakers s
    left join query_4079728 st -- staked NXM per token - base
      on s.pool_id = st.pool_id
      and s.token_id = st.token_id
      and s.pool_token_rn = 1
      and (st.token_date_rn = 1 and st.block_date = current_date) -- today's stake
),

member_activity_combined as (
  select
    m.member,
    if(b.cover_owner is not null, true, false) as is_buyer,
    b.is_active,
    if(nh.address is not null, true, false) as is_nxm_holder,
    nh.amount as nxm_balance,
    if(s.staker is not null, true, false) as is_staker,
    s.pool_id,
    s.token_id,
    s.pool_token_rn,
    s.nxm_active_stake
  from member_whitelist m
    left join buyers b on m.member = b.cover_owner
    left join nxm_holders nh on m.member = nh.address
    left join stakers_active_stake s on m.member = s.staker
),

member_activity_combined_agg as (
  select
    count(distinct member) filter (where is_nxm_holder) as all_time_nxm_holders,
    count(distinct member) filter (where is_nxm_holder and nxm_balance > 1e-11) as current_nxm_holders,
    count(distinct member) filter (where is_buyer) as all_time_buyers,
    count(distinct member) filter (where is_buyer and is_active) as current_active_cover_buyers,
    count(distinct member) filter (where is_staker) as all_time_stakers,
    count(distinct member) filter (where is_staker and nxm_active_stake > 0) as current_stakers
  from member_activity_combined
)

select
  i.item_1,
  case i.item_1
    --when 'latest activity date' then cast(mas.latest_member_activity_date as varchar)
    when 'all time members' then format('%,d', cast(mas.all_time_members as bigint))
    when 'active members' then format('%,d', cast(mas.active_members as bigint))
    when '30d change' then format('%,d', cast(mas.active_members_30d_change as bigint))
    when '30d % change' then format('%.2f%%', cast(mas.active_members_30d_pct_change as double))
    when '90d change' then format('%,d', cast(mas.active_members_90d_change as bigint))
    when '90d % change' then format('%.2f%%', cast(mas.active_members_90d_pct_change as double))
    when '180d change' then format('%,d', cast(mas.active_members_180d_change as bigint))
    when '180d % change' then format('%.2f%%', cast(mas.active_members_180d_pct_change as double))
    when 'all time NXM holders' then format('%,d', cast(mac.all_time_nxm_holders as bigint))
    when 'current NXM holders' then format('%,d', cast(mac.current_nxm_holders as bigint))
    when 'all time buyers' then format('%,d', cast(mac.all_time_buyers as bigint))
    when 'all time stakers' then format('%,d', cast(mac.all_time_stakers as bigint))
  end as value_1,
  i.item_2,
  case i.item_2
    when 'active members' then format('%,d', cast(mas.active_members as bigint))
    when '30d % change' then format('%.2f%%', cast(mas.active_members_30d_pct_change as double))
    when '90d % change' then format('%.2f%%', cast(mas.active_members_90d_pct_change as double))
    when '180d % change' then format('%.2f%%', cast(mas.active_members_180d_pct_change as double))
    when 'current NXM holders' then format('%,d', cast(mac.current_nxm_holders as bigint))
    when 'active cover buyers' then format('%,d', cast(mac.current_active_cover_buyers as bigint))
    when 'current stakers' then format('%,d', cast(mac.current_stakers as bigint))
  end as value_2
from items i
  cross join member_activity_stats mas
  cross join member_activity_combined_agg mac
order by i.id
