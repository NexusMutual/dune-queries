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
  qc.quote,
  qc.v,
  qc.r,
  qc.s,
  qe.evt_tx_hash as tx_hash
from opencover_base.Quote_evt_QuoteSubmitted qe
  inner join opencover_base.Quote_call_submitQuote qc on qe.evt_block_time = qc.call_block_time
    and qe.evt_tx_hash = qc.call_tx_hash
    and qe.quoteId = qc.output_quoteId
where qc.call_success
