with

mints as (
  select
    call_block_time as block_time,
    poolId as pool_id,
    output_id as token_id,
    "to" as to_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingNFT_call_mint
  where call_success
),

transfers as (
  select
    evt_block_time as block_time,
    tokenId as token_id,
    "from" as from_address,
    "to" as to_address,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingNFT_evt_Transfer
),

stakers_base as (
  select
    block_time,
    pool_id,
    token_id,
    staker,
    tx_hash,
    row_number() over (partition by pool_id, token_id order by block_time desc) as event_rn
  from (
    select
      block_time,
      pool_id,
      token_id,
      to_address as staker,
      tx_hash
    from mints
    union all
    select
      t.block_time,
      m.pool_id,
      m.token_id,
      t.to_address as staker,
      t.tx_hash
    from mints m
      inner join transfers t on m.token_id = t.token_id
    where t.from_address <> 0x0000000000000000000000000000000000000000 -- excl mints
  ) t
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

stakers as (
  select
    sb.pool_id,
    sb.token_id,
    sb.staker as staker_address,
    ens.name as staker_ens,
    coalesce(ens.name, cast(sb.staker as varchar)) as staker,
    st.total_staked_nxm as staked_nxm,
    st.total_staked_nxm * p.avg_nxm_eth_price as staked_nxm_eth,
    st.total_staked_nxm * p.avg_nxm_usd_price as staked_nxm_usd
  from stakers_base sb
    inner join query_4079728 st -- staked nxm per token - base
      on sb.pool_id = st.pool_id and sb.token_id = st.token_id
    left join labels.ens on sb.staker = ens.address
    cross join latest_prices p
  where sb.event_rn = 1 -- current staker
    and (st.token_date_rn = 1 and st.block_date = current_date) -- today's stake
)

select
  staker,
  sum(staked_nxm) as staked_nxm,
  sum(staked_nxm_eth) as staked_nxm_eth,
  sum(staked_nxm_usd) as staked_nxm_usd,
  listagg(cast(pool_id as varchar), ',') within group (order by pool_id) as pools,
  listagg(cast(token_id as varchar), ',') within group (order by token_id) as tokens
from stakers
group by 1
--order by 2 desc
