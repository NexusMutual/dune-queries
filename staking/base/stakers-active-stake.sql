with

staking_pool_names as (
  select pool_id, pool_name
  from query_3833996 -- staking pool names base (fallback) query
),

stakers_base as (
  select
    block_time,
    pool_id,
    token_id,
    staker,
    tx_hash,
    pool_token_rn
  from query_5578777 -- stakers - base
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_eth_price, block_date) as avg_nxm_eth_price,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
)

select
  sb.pool_id,
  spn.pool_name,
  sb.token_id,
  st.tranche_id,
  sb.staker as staker_address,
  ens.name as staker_ens,
  coalesce(ens.name, cast(sb.staker as varchar)) as staker,
  st.total_staked_nxm as staked_nxm,
  st.total_staked_nxm * p.avg_nxm_eth_price as staked_nxm_eth,
  st.total_staked_nxm * p.avg_nxm_usd_price as staked_nxm_usd,
  st.stake_expiry_date
from stakers_base sb
  --inner join nexusmutual_ethereum.staked_per_token_tranche st
  inner join query_5226858 st -- staked nxm per token & tranche - base
    on sb.pool_id = st.pool_id and sb.token_id = st.token_id
  left join staking_pool_names spn on sb.pool_id = spn.pool_id
  left join labels.ens on sb.staker = ens.address
  cross join latest_prices p
where sb.pool_token_rn = 1 -- current staker
  and (st.token_tranche_rn = 1 and st.block_date = current_date) -- today's stake
