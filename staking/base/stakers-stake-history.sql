with

stakers_base as (
  select
    block_time,
    pool_id,
    token_id,
    staker,
    tx_hash,
    pool_token_rn
  from query_5578777 -- stakers - base
)

select
  st.block_date,
  sb.pool_token_rn,
  st.token_tranche_rn,
  sb.pool_id,
  sb.token_id,
  st.tranche_id,
  sb.staker,
  st.total_staked_nxm as staked_nxm,
  st.stake_expiry_date
from stakers_base sb
  --inner join nexusmutual_ethereum.staked_per_token_tranche st
  inner join query_5226858 st -- staked nxm per token & tranche - base
    on sb.pool_id = st.pool_id and sb.token_id = st.token_id
--where sb.pool_token_rn = 1 -- current staker
--  and (st.token_tranche_rn = 1 and st.block_date = current_date) -- today's stake
--order by 1,2,3
