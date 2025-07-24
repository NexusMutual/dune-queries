with

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
    st.total_staked_nxm as nxm_active_stake
  from stakers s
    inner join query_4079728 st -- staked NXM per token - base
      on s.pool_id = st.pool_id
      and s.token_id = st.token_id
      and s.pool_token_rn = 1
      and (st.token_date_rn = 1 and st.block_date = current_date) -- today's stake
)

select
  staker,
  sum(nxm_active_stake) as nxm_active_stake
from stakers_active_stake
group by 1
order by 2 desc
