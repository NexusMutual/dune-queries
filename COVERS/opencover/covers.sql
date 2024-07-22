with

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    product_id,
    cover_asset,
    sum_assured,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount,
    tx_hash
  from query_3788370 -- covers v2 base (fallback) query
  --from nexusmutual_ethereum.covers_v2
  where is_migrated = false
),

quotes as (
  select
    blockchain,
    quote_id,
    quote_submitted_sender,
    provider_id,
    product_id,
    cover_asset_id,
    cover_amount,
    payment_asset_id,
    premium_amount,
    quote_settled_block_time,
    mainnet_tx_hash,
    quote_settled_tx_hash
  from query_3933198 -- opencover quotes
  where quote_settled_block_time is not null
)

select
  c.cover_id,
  c.cover_start_time,
  q.quote_settled_block_time
from covers c
  inner join quotes q on c.tx_hash = q.mainnet_tx_hash and c.product_id = q.product_id
