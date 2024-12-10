with

cover_base as (
  select distinct
    cover_id, product_id, product_name, cover_start_date, cover_end_date,
    cover_asset, sum_assured, cover_owner, cover_ipfs_data
  --from query_3788370 -- covers v2 - base
  from nexusmutual_ethereum.covers_v2
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
    product_name,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured,
    cover_owner,
    try(from_hex(trim(wallet))) as cover_data_address
  from cover_ipfs_data_ext c
    cross join unnest(wallets) as w(wallet)
)

select
  cover_id,
  product_id,
  product_name as plan,
  cover_start_date,
  cover_end_date,
  cover_asset,
  sum_assured,
  cover_owner,
  coalesce(cover_data_address, cover_owner) as monitored_wallet
from cover
