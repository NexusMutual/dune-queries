-- RWIV Q8 - Locking Contract
-- Dune query ID: 7363186
-- One row per (memberId, lockId) lock. Shares read directly from SharesLocked events.
-- USDC values calculated from the rate model (SharesLocked only emits shares).
-- Edits: latest state (shares, period) is used; original vs latest timestamps and
-- expiries are both exposed so edits are visible. Assumes the `period` field on
-- SharesLocked carries the TOTAL period after edit (parallel to shares = total
-- after edit). Verify when the first edit occurs; if delta semantics, switch to
-- summing periods across events.

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

lock_events_ranked AS (
  SELECT
    memberId,
    lockId,
    CAST(shares AS double) AS shares,
    CAST(period AS double) AS period,
    evt_block_time,
    ROW_NUMBER() OVER (PARTITION BY memberId, lockId ORDER BY evt_block_time ASC)  AS rn_asc,
    ROW_NUMBER() OVER (PARTITION BY memberId, lockId ORDER BY evt_block_time DESC) AS rn_desc
  FROM nexusmutual_rwiv_ethereum.locks_evt_shareslocked
),

lock_state AS (
  SELECT
    first_evt.memberId,
    first_evt.lockId,
    first_evt.evt_block_time AS original_lock_timestamp,
    first_evt.period         AS original_period,
    last_evt.evt_block_time  AS latest_lock_timestamp,
    last_evt.shares          AS shares,
    last_evt.period          AS latest_period
  FROM lock_events_ranked first_evt
  JOIN lock_events_ranked last_evt
    ON first_evt.memberId = last_evt.memberId
   AND first_evt.lockId   = last_evt.lockId
  WHERE first_evt.rn_asc = 1
    AND last_evt.rn_desc = 1
),

withdrawals AS (
  SELECT DISTINCT
    memberId,
    lockId,
    evt_block_time AS withdrawn_time
  FROM nexusmutual_rwiv_ethereum.locks_evt_shareswithdrawn
),

locks AS (
  SELECT
    ls.original_lock_timestamp,
    ls.latest_lock_timestamp,
    ls.memberId,
    ma.memberAddress,
    ls.shares,
    ls.original_period,
    ls.latest_period,
    date_add('second', CAST(ls.original_period AS integer), ls.original_lock_timestamp) AS original_expiry,
    date_add('second', CAST(ls.latest_period   AS integer), ls.original_lock_timestamp) AS latest_expiry,
    w.withdrawn_time,
    cfg.start_rate,
    cfg.rate_per_second,
    cfg.active_from
  FROM lock_state ls
  CROSS JOIN rate_config cfg
  LEFT JOIN member_addresses ma ON ls.memberId = ma.memberId
  LEFT JOIN withdrawals w ON ls.memberId = w.memberId AND ls.lockId = w.lockId
)

SELECT
  original_lock_timestamp,
  latest_lock_timestamp,
  memberId AS member_id,
  memberAddress AS address,
  CASE
    WHEN withdrawn_time IS NOT NULL THEN 'Withdrawn'
    WHEN date_diff('second', latest_expiry, NOW()) >= 0 THEN 'Unlocked'
    ELSE 'Active'
  END AS status,
  ROUND(shares / 1e6, 2) AS amount_rwiv_locked,
  ROUND(
    shares
      * start_rate
      * power(rate_per_second, date_diff('second', active_from, original_lock_timestamp))
      / 1e18 / 1e6,
    2
  ) AS usdc_value_at_lock,
  latest_period / 86400.0 AS lock_duration_days,
  original_expiry,
  latest_expiry,
  GREATEST(date_diff('day', NOW(), latest_expiry), 0) AS days_remaining,
  ROUND(
    shares
      * start_rate
      * power(rate_per_second, date_diff('second', active_from, latest_expiry))
      / 1e18 / 1e6,
    2
  ) AS usdc_value_at_expiry
FROM locks
ORDER BY original_lock_timestamp DESC
