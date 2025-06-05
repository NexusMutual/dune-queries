with

member_activity as (
  select
    block_time,
    block_date,
    member,
    is_active,  
    active_members,
    all_time_members
  from query_5097910 -- member activity base
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
),

member_activity_stats as (
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
),

buyers as (
  select
    cover_owner,
    cover_sold,
    product_sold,
    first_cover_buy,
    last_cover_buy,
    --== cover ==
    eth_cover,
    coalesce(eth_cover / nullif(cover_sold, 0), 0) as mean_eth_cover,
    median_eth_cover,
    usd_cover,
    coalesce(usd_cover / nullif(cover_sold, 0), 0) as mean_usd_cover,
    median_usd_cover,
    --== fees ==
    eth_premium,
    coalesce(eth_premium / nullif(cover_sold, 0), 0) as mean_eth_premium,
    median_eth_premium,
    usd_premium,
    coalesce(usd_premium / nullif(cover_sold, 0), 0) as mean_usd_premium,
    median_usd_premium
  --from query_3913267 -- BD cover owners base
  from nexusmutual_ethereum.cover_owners_agg
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
    left join query_4079728 st -- staked nxm per token - base
      on s.pool_id = st.pool_id
      and s.token_id = st.token_id
      and s.pool_token_rn = 1
      and (st.token_date_rn = 1 and st.block_date = current_date) -- today's stake
),

member_activity_combined as (
  select
    m.member,
    m.is_active,
    if(b.cover_owner is not null, true, false) as is_buyer,
    b.cover_sold,
    b.product_sold,
    b.first_cover_buy,
    b.last_cover_buy,
    b.eth_cover,
    b.usd_cover,
    b.eth_premium,
    b.usd_premium,
    if(nh.address is not null, true, false) as is_nxm_holder,
    nh.amount as nxm_balance,
    if(s.staker is not null, true, false) as is_staker,
    s.pool_id,
    s.token_id,
    s.pool_token_rn,
    s.nxm_active_stake
  from member_activity m
    left join buyers b on m.member = b.cover_owner
    left join nxm_holders nh on m.member = nh.address
    left join stakers_active_stake s on m.member = s.staker
),

member_activity_combined_agg as (
  select
    --count(distinct member) as all_time_members,
    --count(distinct member) filter (where is_active) as active_members,
    count(distinct member) filter (where is_nxm_holder) as all_time_nxm_holders,
    count(distinct member) filter (where is_nxm_holder and nxm_balance > 1e-11) as current_nxm_holders,
    count(distinct member) filter (where is_buyer) as all_time_buyers,
    count(distinct member) filter (where is_staker) as all_time_stakers,
    count(distinct member) filter (where is_staker and nxm_active_stake > 0) as current_stakers
  from member_activity_combined
)

select
  latest_member_activity_date,
  all_time_members,
  active_members,
  active_members_30d_change,
  active_members_30d_pct_change,
  active_members_90d_change,
  active_members_90d_pct_change,
  active_members_180d_change,
  active_members_180d_pct_change,
  all_time_nxm_holders,
  current_nxm_holders,
  all_time_buyers,
  all_time_stakers,
  current_stakers
from member_activity_combined_agg, member_activity_stats
