with

daily_avg_prices as (
  select
    block_date,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

covers as (
  select
    block_time,
    block_date,
    block_number,
    cover_id,
    tx_hash
  from query_4599092 -- covers v2 - base root (fallback query)
  where is_migrated = false
    and premium_asset = 'NXM'
)

select
  c.block_date,
  c.cover_id,
  bf.amount / 1e18 as nxm_cover_buy_burn,
  bf.amount / 1e18 * 0.5 as nxm_rewards_mint,
  c.tx_hash
from covers c
  inner join nexusmutual_ethereum.tokencontroller_call_burnfrom bf
    on c.block_time = bf.call_block_time
    and c.block_number = bf.call_block_number
    and c.tx_hash = bf.call_tx_hash
