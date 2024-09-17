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

stakers as (
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
)

select
  block_time,
  pool_id,
  token_id,
  staker,
  tx_hash,
  row_number() over (partition by pool_id, token_id order by block_time desc) as event_rn
from stakers
