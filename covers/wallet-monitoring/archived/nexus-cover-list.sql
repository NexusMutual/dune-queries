with

cover_base as (
  select distinct
    cover_id, product_id, product_name, cover_start_date, cover_end_date,
    cover_asset, sum_assured, cover_owner, cover_ipfs_data
  from query_4599092 -- covers v2 - base root (fallback query)
  where product_id in (245, 246, 247) -- Entry / Essential / Elite Plan
),

cover_ipfs_data as (
  select
    cover_id, product_id, product_name,
    cover_start_date, cover_end_date,
    cover_asset, sum_assured, cover_owner,
    http_get(concat('https://api.nexusmutual.io/ipfs/', cover_ipfs_data)) as cover_data
  from cover_base
),

cover_ipfs_data_ext as (
  select
    cover_id, product_id, product_name,
    cover_start_date, cover_end_date,
    cover_asset, sum_assured, cover_owner,
    regexp_split(json_extract_scalar(cover_data, '$.walletAddresses'), ',') as wallets
  from cover_ipfs_data
),

cover as (
  select
    cover_id,
    product_id,
    product_name as plan,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured as native_cover_amount,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', sum_assured, 0) as cbbtc_cover_amount,
    cover_owner,
    try(from_hex(trim(wallet))) as cover_data_address
  from cover_ipfs_data_ext c
    cross join unnest(wallets) as w(wallet)
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_eth_usd_price, block_date) as avg_eth_usd_price,
    max_by(avg_dai_usd_price, block_date) as avg_dai_usd_price,
    max_by(avg_usdc_usd_price, block_date) as avg_usdc_usd_price,
    max_by(avg_cbbtc_usd_price, block_date) as avg_cbbtc_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

cover_enriched as (
  select
    c.cover_id,
    c.product_id,
    c.plan,
    c.cover_start_date,
    c.cover_end_date,
    c.cover_asset,
    c.native_cover_amount,
    c.eth_cover_amount as eth_eth_cover_amount,
    c.eth_cover_amount * p.avg_eth_usd_price as eth_usd_cover_amount,
    c.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_cover_amount,
    c.dai_cover_amount * p.avg_dai_usd_price as dai_usd_cover_amount,
    c.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price as usdc_eth_cover_amount,
    c.usdc_cover_amount * p.avg_usdc_usd_price as usdc_usd_cover_amount,
    c.cbbtc_cover_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price as cbbtc_eth_cover_amount,
    c.cbbtc_cover_amount * p.avg_cbbtc_usd_price as cbbtc_usd_cover_amount,
    c.cover_owner,
    c.cover_data_address
  from cover c
    cross join latest_prices p
)

select
  cover_id,
  product_id,
  plan,
  cover_start_date,
  cover_end_date,
  cover_asset,
  native_cover_amount,
  eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount as usd_cover_amount,
  eth_eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount + cbbtc_eth_cover_amount as eth_cover_amount,
  cover_owner,
  coalesce(cover_data_address, cover_owner) as monitored_wallet,
  current_timestamp as inserted_at
from cover_enriched
where cover_id > {{max_cover_id}}
order by 1
