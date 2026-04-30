-- RWIV Q5 - Deposit Queue
-- Dune query ID: 6973877
-- Tracks all deposit requests and their status in the RWIV vault.
-- All values read directly from event emissions (no rate model needed).
-- Instant vs VO Approved determined by comparing tx hashes between request and fulfillment.

WITH member_addresses AS (
  SELECT DISTINCT memberId, memberAddress
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  UNION
  SELECT DISTINCT memberId, memberAddress
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
),

deposit_requests AS (
  SELECT
    requestId,
    memberId,
    CAST(assets AS double) AS assets,
    evt_block_time,
    evt_tx_hash
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositrequested
),

deposit_fulfillments AS (
  SELECT
    requestId,
    SUM(CAST(assets AS double)) AS fulfilled_assets,
    SUM(CAST(shares AS double)) AS fulfilled_shares,
    MAX(evt_block_time) AS last_fulfilment_time,
    MIN(evt_tx_hash) AS fulfilment_tx_hash
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  GROUP BY requestId
),

deposit_cancellations AS (
  SELECT
    requestId,
    evt_block_time AS cancel_time
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositrequestcanceled
),

deposit_queue AS (
  SELECT
    dr.evt_block_time AS request_timestamp,
    dr.requestId AS request_id,
    ma.memberAddress AS address,
    dr.assets AS requested_assets,
    COALESCE(df.fulfilled_assets, 0) AS fulfilled_assets,
    COALESCE(df.fulfilled_shares, 0) AS fulfilled_shares,
    df.last_fulfilment_time,
    df.fulfilment_tx_hash,
    dr.evt_tx_hash AS request_tx_hash,
    dc.cancel_time
  FROM deposit_requests dr
  LEFT JOIN member_addresses ma ON dr.memberId = ma.memberId
  LEFT JOIN deposit_fulfillments df ON dr.requestId = df.requestId
  LEFT JOIN deposit_cancellations dc ON dr.requestId = dc.requestId
)

SELECT
  request_timestamp,
  request_id,
  address,
  CASE
    WHEN fulfilled_assets > 0 THEN 'Filled'
    WHEN cancel_time IS NOT NULL THEN 'Canceled'
    ELSE 'Active'
  END AS status,
  ROUND(requested_assets / 1e6, 2) AS request_size_usdc,
  CASE
    WHEN fulfilment_tx_hash IS NULL THEN 'VO Action Pending'
    WHEN fulfilment_tx_hash = request_tx_hash THEN 'Instant'
    ELSE 'VO Approved'
  END AS instant_or_vo,
  ROUND((requested_assets - fulfilled_assets) / 1e6, 2) AS remaining_usdc,
  ROUND(fulfilled_assets / 1e6, 2) AS filled_usdc,
  ROUND(fulfilled_shares / 1e6, 2) AS rwiv_issued,
  last_fulfilment_time AS fulfilment_timestamp
FROM deposit_queue
ORDER BY request_timestamp DESC
