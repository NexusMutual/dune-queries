with

covers as (
  select distinct
    cover_id,
    buy_type,
    cover_start_date,
    cover_end_date,
    cast(original_cover_end_time as date) as original_cover_end_date,
    original_cover_id,
    new_cover_id,
    cover_owner,
    product_id,
    product_type,
    product_name,
    cover_asset,
    sum_assured as cover_amount,
    sum(original_premium) over (partition by cover_id) as original_premium,
    sum(premium_incl_commission) over (partition by cover_id) as premium
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
  if(starts_with(c.buy_type, 'edit'), 'edit', c.buy_type) as buy_type,
  --cr.limit_order_id,
  ce.product_id,
  co.product_name,
  ce.original_cover_id as orig_cover_id,
  ce.cover_id as new_cover_id,
  c.cover_asset,
  co.cover_amount as orig_cover,
  c.cover_amount as new_cover,
  co.original_premium as orig_premium,
  co.premium as current_premium,
  c.premium as new_premium,
  co.cover_start_date as orig_start_date,
  co.original_cover_end_date as orig_end_date,
  co.cover_end_date as current_end_date,
  c.cover_start_date as new_start_date,
  c.cover_end_date as new_end_date,
  ce.buyer
from cover_edits ce
  inner join covers co on ce.original_cover_id = co.cover_id
  inner join covers c on ce.cover_id = c.cover_id
  --left join cover_renewals cr on ce.cover_id = cr.cover_id and ce.original_cover_id = cr.original_cover_id and ce.buyer = cr.buyer
order by 1 desc
