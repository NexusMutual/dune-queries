with

whitelist as (
  select
    call_block_time as block_time,
    _member as member,
    true as is_whitelisted
  from nexusmutual_ethereum.tokencontroller_call_addtowhitelist
  where call_success
  union all
  select
    call_block_time as block_time,
    _member as member,
    false as is_whitelisted
  from nexusmutual_ethereum.tokencontroller_call_removefromwhitelist
  where call_success  
),

whitelist_ordered as (
  select
    *,
    row_number() over (partition by member order by block_time desc) as rn
  from whitelist
),

members as (
  select block_time, member, is_whitelisted
  from whitelist_ordered
  where rn = 1
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
    staker,
    array_agg(distinct pool_id) as pool_ids,
    array_agg(distinct token_id) as token_ids
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
  group by 1
),

member_activity_combined as (
  select
    m.member,
    m.is_whitelisted as is_active,
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
    s.pool_ids,
    s.token_ids
  from members m
    left join buyers b on m.member = b.cover_owner
    left join nxm_holders nh on m.member = nh.address
    left join stakers s on m.member = s.staker
)

select
  count(distinct member) as all_time_members,
  count(distinct member) filter (where is_active) as active_members,
  count(distinct member) filter (where is_nxm_holder) as all_time_nxm_holders,
  count(distinct member) filter (where is_nxm_holder and nxm_balance > 1e-11) as current_nxm_holders,
  count(distinct member) filter (where is_buyer) as all_time_buyers,
  count(distinct member) filter (where is_staker) as all_time_stakers
from member_activity_combined
