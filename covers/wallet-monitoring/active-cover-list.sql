with

cover_base as (
  select distinct
    cover_id, product_id, product_name, product_type, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner, cover_ipfs_data
  from query_4599092 -- covers v2 - base root
  where cover_end_date >= current_date
    --and product_id in (227, 245, 246, 247) -- Base DeFi Pass / Entry / Essential / Elite Plan
    and cover_id > {{max_cover_id}}
),

cover_raw as (
  select
    cover_id, product_id, product_name, product_type, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner,
    http_get(concat('https://api.nexusmutual.io/ipfs/', cover_ipfs_data)) as cover_data,
    cover_ipfs_data
  from cover_base
),

parsed as (
  select
    *,
    try(json_parse(cover_data)) as j
  from cover_raw
),

addr_arrays as (
  select
    cover_id, product_id, product_name, product_type, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner,
    cover_data, cover_ipfs_data,
    -- arrays of strings at common keys
    coalesce(try(cast(json_extract(j, '$.wallets') as array(varchar))), cast(array[] as array(varchar))) as a_wallets,
    -- arrays of objects with .address
    coalesce(try(transform(cast(json_extract(j, '$.wallets') as array(json)), x -> json_extract_scalar(x, '$.address'))), cast(array[] as array(varchar))) as ao_wallets,
    -- maps keyed by address
    coalesce(try(map_keys(cast(json_extract(j, '$.wallets') as map(varchar, json)))), cast(array[] as array(varchar))) as mk_wallets,
    -- single-address fallbacks
    coalesce(
      try(filter(array[
        nullif(json_extract_scalar(j, '$.wallet'), '')
      ], x -> x is not null)),
      cast(array[] as array(varchar))
    ) as singles,
    -- regex fallback: grab all 0x...40 hex from raw json text
    coalesce(
      try(regexp_extract_all(coalesce(cover_data, ''), '0x[0-9a-fA-F]{40}')),
      cast(array[] as array(varchar))
    ) as re_all
  from parsed
),

wallets_combined as (
  select
    cover_id, product_id, product_name, product_type, cover_start_date, cover_end_date, cover_asset, sum_assured, cover_owner,
    cover_data, cover_ipfs_data,
    array_distinct(
      filter(
        concat(a_wallets, ao_wallets, mk_wallets, singles, re_all),
        x -> x is not null and length(trim(x)) > 0
      )
    ) as wallets_all
  from addr_arrays
),

wallets_unnested as (
  select
    w.cover_id,
    w.product_id,
    if(w.product_id in (227, 245, 246, 247, 273, 296, 297, 298), w.product_name) as plan,
    w.product_name,
    w.product_type,
    w.cover_start_date,
    w.cover_end_date,
    w.cover_asset,
    w.sum_assured as native_cover_amount,
    if(w.cover_asset = 'ETH', w.sum_assured, 0) as eth_cover_amount,
    if(w.cover_asset = 'DAI', w.sum_assured, 0) as dai_cover_amount,
    if(w.cover_asset = 'USDC', w.sum_assured, 0) as usdc_cover_amount,
    if(w.cover_asset = 'cbBTC', w.sum_assured, 0) as cbbtc_cover_amount,
    w.cover_owner,
    try(from_hex(trim(u.wallet))) as wallet,
    w.cover_data,
    w.cover_ipfs_data
  from wallets_combined w
    left join unnest(
      case when cardinality(w.wallets_all) = 0 then array[cast(null as varchar)] else w.wallets_all end
    ) as u(wallet) on true
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
    c.product_name,
    c.product_type,
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
    c.wallet
  from wallets_unnested c
    cross join latest_prices p
)

select
  cover_id,
  product_id,
  plan,
  product_name,
  product_type,
  cover_start_date,
  cover_end_date,
  cover_asset,
  native_cover_amount,
  eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount as usd_cover_amount,
  eth_eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount + cbbtc_eth_cover_amount as eth_cover_amount,
  cover_owner,
  coalesce(wallet, cover_owner) as monitored_wallet,
  current_timestamp as inserted_at
from cover_enriched
order by 1
