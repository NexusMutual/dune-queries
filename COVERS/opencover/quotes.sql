with

quotes_submitted as (
  select
    'arbitrum' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    cast(json_query(qc.quote, 'lax $.providerId') as int) as provider_id,
    cast(json_query(qc.quote, 'lax $.productId') as int) as product_id,
    cast(json_query(qc.quote, 'lax $.coverAssetId') as int) as cover_asset_id,
    cast(json_query(qc.quote, 'lax $.coverAmount') as uint256) as cover_amount,
    cast(json_query(qc.quote, 'lax $.paymentAssetId') as int) as payment_asset_id,
    cast(json_query(qc.quote, 'lax $.premiumAmount') as uint256) as premium_amount,
    cast(json_query(qc.quote, 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query(qc.quote, 'lax $.coverExpiry') as int) as cover_expiry,
    cast(json_query(qc.quote, 'lax $.validUntil') as bigint) as valid_until,
    qe.evt_tx_hash as tx_hash
  from opencover_arbitrum.Quote_evt_QuoteSubmitted qe
    inner join opencover_arbitrum.Quote_call_submitQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.output_quoteId
  where qc.call_success
  union all
  select
    'base' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    cast(json_query(qc.quote, 'lax $.providerId') as int) as provider_id,
    cast(json_query(qc.quote, 'lax $.productId') as int) as product_id,
    cast(json_query(qc.quote, 'lax $.coverAssetId') as int) as cover_asset_id,
    cast(json_query(qc.quote, 'lax $.coverAmount') as uint256) as cover_amount,
    cast(json_query(qc.quote, 'lax $.paymentAssetId') as int) as payment_asset_id,
    cast(json_query(qc.quote, 'lax $.premiumAmount') as uint256) as premium_amount,
    cast(json_query(qc.quote, 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query(qc.quote, 'lax $.coverExpiry') as int) as cover_expiry,
    cast(json_query(qc.quote, 'lax $.validUntil') as bigint) as valid_until,
    qe.evt_tx_hash as tx_hash
  from opencover_base.Quote_evt_QuoteSubmitted qe
    inner join opencover_base.Quote_call_submitQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.output_quoteId
  where qc.call_success
  union all
  select
    'optimism' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    cast(json_query(qc.quote, 'lax $.providerId') as int) as provider_id,
    cast(json_query(qc.quote, 'lax $.productId') as int) as product_id,
    cast(json_query(qc.quote, 'lax $.coverAssetId') as int) as cover_asset_id,
    cast(json_query(qc.quote, 'lax $.coverAmount') as uint256) as cover_amount,
    cast(json_query(qc.quote, 'lax $.paymentAssetId') as int) as payment_asset_id,
    cast(json_query(qc.quote, 'lax $.premiumAmount') as uint256) as premium_amount,
    cast(json_query(qc.quote, 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query(qc.quote, 'lax $.coverExpiry') as int) as cover_expiry,
    cast(json_query(qc.quote, 'lax $.validUntil') as bigint) as valid_until,
    qe.evt_tx_hash as tx_hash
  from opencover_optimism.Quote_evt_QuoteSubmitted qe
    inner join opencover_optimism.Quote_call_submitQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.output_quoteId
  where qc.call_success
  union all
  select
    'polygon' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    cast(json_query(qc.quote, 'lax $.providerId') as int) as provider_id,
    cast(json_query(qc.quote, 'lax $.productId') as int) as product_id,
    cast(json_query(qc.quote, 'lax $.coverAssetId') as int) as cover_asset_id,
    cast(json_query(qc.quote, 'lax $.coverAmount') as uint256) as cover_amount,
    cast(json_query(qc.quote, 'lax $.paymentAssetId') as int) as payment_asset_id,
    cast(json_query(qc.quote, 'lax $.premiumAmount') as uint256) as premium_amount,
    cast(json_query(qc.quote, 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query(qc.quote, 'lax $.coverExpiry') as int) as cover_expiry,
    cast(json_query(qc.quote, 'lax $.validUntil') as bigint) as valid_until,
    qe.evt_tx_hash as tx_hash
  from opencover_polygon.Quote_evt_QuoteSubmitted qe
    inner join opencover_polygon.Quote_call_submitQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.output_quoteId
  where qc.call_success
),

quotes_settled as (
  select
    'arbitrum' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    qc.coverExpiresAt as cover_expires_at,
    qc.txHash as mainnet_tx_hash,
    qe.evt_tx_hash as tx_hash
  from opencover_arbitrum.Quote_evt_QuoteSettled qe
    inner join opencover_arbitrum.Quote_call_settleQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.quoteId
  where qc.call_success
  union all
  select
    'base' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    qc.coverExpiresAt as cover_expires_at,
    qc.txHash as mainnet_tx_hash,
    qe.evt_tx_hash as tx_hash
  from opencover_base.Quote_evt_QuoteSettled qe
    inner join opencover_base.Quote_call_settleQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.quoteId
  where qc.call_success
  union all
  select
    'optimism' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    qc.coverExpiresAt as cover_expires_at,
    qc.txHash as mainnet_tx_hash,
    qe.evt_tx_hash as tx_hash
  from opencover_optimism.Quote_evt_QuoteSettled qe
    inner join opencover_optimism.Quote_call_settleQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.quoteId
  where qc.call_success
  union all
  select
    'polygon' as blockchain,
    qe.evt_block_time as block_time,
    qe.evt_block_number as block_number,
    qe.quoteId as quote_id,
    qe.sender,
    qc.coverExpiresAt as cover_expires_at,
    qc.txHash as mainnet_tx_hash,
    qe.evt_tx_hash as tx_hash
  from opencover_polygon.Quote_evt_QuoteSettled qe
    inner join opencover_polygon.Quote_call_settleQuote qc on qe.evt_block_time = qc.call_block_time
      and qe.evt_tx_hash = qc.call_tx_hash
      and qe.quoteId = qc.quoteId
  where qc.call_success
)

select
  sb.blockchain,
  sb.block_time as quote_submitted_block_time,
  sb.block_number as quote_submitted_block_number,
  sb.quote_id,
  sb.sender as quote_submitted_sender,
  sb.provider_id,
  sb.product_id,
  sb.cover_asset_id,
  sb.cover_amount,
  sb.payment_asset_id,
  sb.premium_amount,
  sb.fee_amount,
  sb.cover_expiry,
  sb.valid_until,
  sb.tx_hash as quote_submitted_tx_hash,
  st.block_time as quote_settled_block_time,
  st.block_number as quote_settled_block_number,
  st.sender as quote_settled_sender,
  st.cover_expires_at,
  st.mainnet_tx_hash,
  st.tx_hash as quote_settled_tx_hash
from quotes_submitted sb
  left join quotes_settled st on sb.blockchain = st.blockchain and sb.quote_id = st.quote_id
