-- RWIV Q4a - Withdrawal Queue (Live)
-- Dune query ID: 6976835
-- Shows only pending withdrawal requests (not fully filled, not canceled).
-- Sorted earliest-first (FIFO queue order).
-- Subset of Q4 (Withdrawals).

WITH rate_config AS (
  -- Single source of truth: Base Query 0 (Dune 7392734).
  SELECT start_rate, rate_per_second, active_from FROM query_7392734
),

member_addresses AS (
  SELECT DISTINCT memberId, memberAddress
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  UNION
  SELECT DISTINCT memberId, memberAddress
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
),

redeem_requests AS (
  SELECT
    requestId,
    memberId,
    CAST(shares AS double) AS shares,
    evt_block_time
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemrequested
),

redeem_fulfillments AS (
  SELECT
    requestId,
    SUM(CAST(assets AS double)) AS fulfilled_assets,
    SUM(CAST(shares AS double)) AS fulfilled_shares,
    MAX(evt_block_time) AS last_fulfilment_time
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
  GROUP BY requestId
),

redeem_cancellations AS (
  SELECT
    requestId,
    evt_block_time AS cancel_time
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemrequestcanceled
),

withdrawal_queue AS (
  SELECT
    rr.evt_block_time AS request_timestamp,
    rr.requestId AS request_id,
    ma.memberAddress AS address,
    rr.shares AS requested_shares,
    COALESCE(rf.fulfilled_shares, 0) AS fulfilled_shares,
    COALESCE(rf.fulfilled_assets, 0) AS fulfilled_assets,
    rf.last_fulfilment_time,
    rc.cancel_time,
    cfg.start_rate,
    cfg.rate_per_second,
    cfg.active_from
  FROM redeem_requests rr
  CROSS JOIN rate_config cfg
  LEFT JOIN member_addresses ma ON rr.memberId = ma.memberId
  LEFT JOIN redeem_fulfillments rf ON rr.requestId = rf.requestId
  LEFT JOIN redeem_cancellations rc ON rr.requestId = rc.requestId
  WHERE COALESCE(rf.fulfilled_shares, 0) < rr.shares
    AND rc.requestId IS NULL
)

SELECT
  request_timestamp,
  request_id,
  address,
  CASE
    WHEN fulfilled_shares > 0 THEN 'Partially Filled'
    ELSE 'Unfilled'
  END AS status,
  ROUND(
    requested_shares
      * start_rate
      * power(rate_per_second, date_diff('second', active_from, request_timestamp))
      / 1e18 / 1e6,
    2
  ) AS request_size_usdc,
  ROUND(requested_shares / 1e6, 2) AS request_size_rwiv,
  ROUND(fulfilled_assets / 1e6, 2) AS filled_usdc,
  ROUND(
    (requested_shares - fulfilled_shares)
      * start_rate
      * power(rate_per_second, date_diff('second', active_from, NOW()))
      / 1e18 / 1e6,
    2
  ) AS remaining_usdc,
  last_fulfilment_time AS fulfilment_timestamp
FROM withdrawal_queue
ORDER BY request_timestamp ASC
