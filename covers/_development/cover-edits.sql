with

covers as (
  select
    cover_id,
    buy_type,
    cover_start_time,
    cover_end_time,
    cover_period_seconds,
    cover_start_date,
    cover_end_date,
    original_cover_end_time,
    original_cover_id,
    new_cover_id,
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
    --amount, -- always null
    coverId as cover_id,
    originalCoverId as original_cover_id,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.cover_evt_coverbought
  where coverId <> originalCoverId
)

/*cover_renewals as (
  select
    evt_block_time as block_time,
    originalCoverId as original_cover_id,
    coverId as cover_id,
    owner as buyer,
    id as limit_order_id,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.limitorders_evt_orderexecuted
)*/

select
  ce.block_time,
  c.buy_type,
  --cr.limit_order_id,
  ce.buyer,
  ce.product_id,
  ce.original_cover_id,
  co.cover_start_time as original_cover_start_time,
  co.cover_end_time as current_cover_end_time,
  co.original_cover_end_time,
  ce.cover_id,
  c.cover_start_time,
  c.cover_end_time,
  co.cover_period_seconds as original_cover_period_seconds,
  c.cover_period_seconds,
  co.staking_pool_id as original_staking_pool_id,
  c.staking_pool_id,
  co.cover_asset as original_cover_asset,
  c.cover_asset,
  co.sum_assured as original_sum_assured,
  c.sum_assured
from cover_edits ce
  inner join covers co on ce.original_cover_id = co.cover_id
  inner join covers c on ce.cover_id = c.cover_id
  --left join cover_renewals cr on ce.cover_id = cr.cover_id and ce.original_cover_id = cr.original_cover_id and ce.buyer = cr.buyer
