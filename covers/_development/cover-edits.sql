with

covers as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    cover_owner,
    staking_pool_id,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    partial_cover_amount,
    sum(partial_cover_amount) over (partition by cover_id) as total_cover_amount,
    premium_incl_commission as premium_nxm
  from query_4599092 -- covers v2 - base root (fallback query)
),

cover_edits as (
  select
    evt_block_time as block_time,
    buyer,
    productId as product_id,
    amount,
    coverId as cover_id,
    originalCoverId as original_cover_id,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.cover_evt_coverbought
  where coverId <> originalCoverId
)

select
  ce.block_time,
  ce.buyer,
  ce.product_id,
  ce.cover_id,
  ce.original_cover_id,
  co.cover_start_time as original_cover_start_time,
  co.cover_end_time as original_cover_end_time,
  c.cover_start_time,
  c.cover_end_time,
  co.staking_pool_id as original_staking_pool_id,
  c.staking_pool_id,
  co.cover_asset as original_cover_asset,
  c.cover_asset,
  co.sum_assured as original_sum_assured,
  c.sum_assured
from cover_edits ce
  inner join covers co on ce.original_cover_id = co.cover_id
  inner join covers c on ce.cover_id = c.cover_id
