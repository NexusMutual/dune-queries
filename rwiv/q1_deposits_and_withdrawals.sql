-- RWIV Q1 - Deposits and Withdrawals
-- Dune query ID: 6966602
-- Tracks all successful deposits and withdrawals (requested and filled) in the RWIV vault.
-- Values read directly from event emissions where available.
-- withdrawal_requested_usdc is calculated from the rate model (event only emits shares).

WITH rate_config AS (
  -- Single source of truth: Base Query 0 (Dune 7392734).
  SELECT start_rate, rate_per_second, active_from FROM query_7392734
),

member_addresses AS (
  SELECT DISTINCT memberId, memberAddress
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
),

deposits AS (
  SELECT
    evt_block_time AS block_time,
    memberAddress AS user_address,
    ROUND(CAST(assets AS double) / 1e6, 2) AS deposit_usdc,
    ROUND(CAST(shares AS double) / 1e6, 2) AS deposit_rwiv,
    CAST(NULL AS double) AS withdrawal_requested_usdc,
    CAST(NULL AS double) AS withdrawal_requested_rwiv,
    CAST(NULL AS double) AS withdrawal_filled_usdc,
    CAST(NULL AS double) AS withdrawal_filled_rwiv
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
),

redeem_requests AS (
  SELECT
    r.evt_block_time AS block_time,
    ma.memberAddress AS user_address,
    CAST(NULL AS double) AS deposit_usdc,
    CAST(NULL AS double) AS deposit_rwiv,
    ROUND(
      CAST(r.shares AS double)
        * rc.start_rate
        * power(rc.rate_per_second, date_diff('second', rc.active_from, r.evt_block_time))
        / 1e18 / 1e6,
      2
    ) AS withdrawal_requested_usdc,
    ROUND(CAST(r.shares AS double) / 1e6, 2) AS withdrawal_requested_rwiv,
    CAST(NULL AS double) AS withdrawal_filled_usdc,
    CAST(NULL AS double) AS withdrawal_filled_rwiv
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemrequested r
  CROSS JOIN rate_config rc
  LEFT JOIN member_addresses ma ON r.memberId = ma.memberId
),

redeem_fills AS (
  SELECT
    evt_block_time AS block_time,
    memberAddress AS user_address,
    CAST(NULL AS double) AS deposit_usdc,
    CAST(NULL AS double) AS deposit_rwiv,
    CAST(NULL AS double) AS withdrawal_requested_usdc,
    CAST(NULL AS double) AS withdrawal_requested_rwiv,
    ROUND(CAST(assets AS double) / 1e6, 2) AS withdrawal_filled_usdc,
    ROUND(CAST(shares AS double) / 1e6, 2) AS withdrawal_filled_rwiv
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
)

SELECT * FROM deposits
UNION ALL
SELECT * FROM redeem_requests
UNION ALL
SELECT * FROM redeem_fills
ORDER BY block_time DESC
