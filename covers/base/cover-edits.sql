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
  from query_4599092 -- covers v2 - base root
),

edit_chain as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    evt_index as evt_index,
    originalCoverId as original_cover_id,
    coalesce(lag(coverId) over (partition by originalCoverId order by evt_block_number, evt_index), originalCoverId) as prev_cover_id,
    coverId as new_cover_id,
    row_number() over (partition by originalCoverId order by evt_block_number, evt_index) as step,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.cover_evt_coverbought
  where coverId <> originalCoverId
)

select
  ec.block_time,
  ec.original_cover_id as orig_cover_id,
  ec.prev_cover_id as prev_cover_id,
  ec.new_cover_id as new_cover_id,
  ec.step,
  co.product_id,
  co.product_name,
  if(starts_with(c.buy_type, 'edit'), 'edit', c.buy_type) as buy_type,
  co.cover_asset,
  co.cover_amount as prev_cover,
  c.cover_amount as new_cover,
  co.original_premium as prev_orig_premium,
  co.premium as prev_current_premium,
  c.premium as new_premium,
  co.cover_start_date as prev_start_date,
  co.original_cover_end_date as prev_end_date,
  co.cover_end_date as prev_current_end_date,
  c.cover_start_date as new_start_date,
  c.cover_end_date as new_end_date,
  co.cover_owner,
  ec.tx_hash
from edit_chain ec
  inner join covers co on ec.prev_cover_id = co.cover_id
  inner join covers c on ec.new_cover_id = c.cover_id
order by ec.original_cover_id desc, ec.step
