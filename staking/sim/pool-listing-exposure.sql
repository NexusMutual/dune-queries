with

active_covers as (
  select
    cover_id,
    cast(staking_pool_id as int) as pool_id,
    product_id,
    product_type,
    product_name as listing,
    cover_asset,
    --ETH
    eth_cover_amount,
    eth_usd_cover_amount,
    --DAI
    dai_eth_cover_amount,
    dai_usd_cover_amount,
    --USDC
    usdc_eth_cover_amount,
    usdc_usd_cover_amount,
    --cbBTC
    cbbtc_eth_cover_amount,
    cbbtc_usd_cover_amount
  from query_5785377 -- active covers - base root
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

exposure_agg as (
  select
    pool_id,
    product_id,
    product_type,
    listing,
    sum(usdc_usd_cover_amount + cbbtc_usd_cover_amount + dai_usd_cover_amount + eth_usd_cover_amount) as usd_cover_amount,
    sum(usdc_eth_cover_amount + cbbtc_eth_cover_amount + dai_eth_cover_amount + eth_cover_amount) as eth_cover_amount
  from active_covers
  group by 1, 2, 3, 4
)

select
  e.pool_id,
  e.product_id,
  e.product_type,
  e.listing,
  e.usd_cover_amount,
  e.eth_cover_amount,
  e.usd_cover_amount / p.avg_nxm_usd_price as nxm_cover_amount
from exposure_agg e
  cross join latest_prices p
order by 1, 2
