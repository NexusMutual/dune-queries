-- RWIV Q11 - Event Log
-- Dune query ID: 8915349
-- One row per deposit, lock and redemption, newest first. Deposits and redemptions are
-- fulfilment events only (requests and cancellations are not deposits or redemptions).
-- Values read directly from event emissions; no rate roll-up. USDC is shown only for events
-- that emit assets, RWIV only for events that emit shares.
-- Lock rows: amount_rwiv = shares in the lock after the event (SharesLocked emits the total);
-- lock_top_up_rwiv / lock_period_added_days = what this event added (SharesLocked emits the
-- added period on edits, not the total). lock_expiry = original start + sum of periods so far
-- (editLock never changes startTime). Lock IDs are array indexes and are never reused.
-- Lock withdrawn rows: SharesWithdrawn emits no amount, so amount_rwiv is the lock's last shares.

WITH member_addresses AS (
  -- Latest known address per member (memberAddress is only emitted on fulfilment).
  SELECT memberId, memberAddress
  FROM (
    SELECT
      memberId,
      memberAddress,
      ROW_NUMBER() OVER (PARTITION BY memberId ORDER BY evt_block_time DESC) AS rn
    FROM (
      SELECT memberId, memberAddress, evt_block_time FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
      UNION ALL
      SELECT memberId, memberAddress, evt_block_time FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
    ) fulfilments
  ) ranked
  WHERE rn = 1
),

lock_events AS (
  SELECT
    evt_block_time,
    evt_tx_hash,
    evt_index,
    memberId,
    lockId,
    CAST(shares AS double) AS shares,
    CAST(topUpShares AS double) AS top_up_shares,
    CAST(period AS double) AS period,
    ROW_NUMBER() OVER (PARTITION BY memberId, lockId ORDER BY evt_block_time, evt_index) AS event_seq,
    ROW_NUMBER() OVER (PARTITION BY memberId, lockId ORDER BY evt_block_time DESC, evt_index DESC) AS rn_desc,
    MIN(evt_block_time) OVER (PARTITION BY memberId, lockId) AS lock_start_time,
    SUM(CAST(period AS double)) OVER (
      PARTITION BY memberId, lockId ORDER BY evt_block_time, evt_index
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS total_period
  FROM nexusmutual_rwiv_ethereum.locks_evt_shareslocked
),

events AS (
  SELECT
    evt_block_time AS block_time,
    evt_index,
    'Deposit' AS event,
    CAST(memberId AS bigint) AS member_id,
    memberAddress AS emitted_address,
    CAST(requestId AS bigint) AS request_id,
    CAST(NULL AS bigint) AS lock_id,
    CAST(assets AS double) / 1e6 AS amount_usdc,
    CAST(shares AS double) / 1e6 AS amount_rwiv,
    CAST(NULL AS double) AS lock_top_up_rwiv,
    CAST(NULL AS double) AS lock_period_added_days,
    CAST(NULL AS timestamp(3) with time zone) AS lock_expiry,
    evt_tx_hash AS tx_hash
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  UNION ALL
  SELECT
    evt_block_time,
    evt_index,
    CASE WHEN event_seq = 1 THEN 'Lock created' ELSE 'Lock edited' END,
    CAST(memberId AS bigint),
    NULL,
    NULL,
    CAST(lockId AS bigint),
    NULL,
    shares / 1e6,
    top_up_shares / 1e6,
    period / 86400.0,
    date_add('second', CAST(total_period AS bigint), lock_start_time),
    evt_tx_hash
  FROM lock_events
  UNION ALL
  SELECT
    w.evt_block_time,
    w.evt_index,
    'Lock withdrawn',
    CAST(w.memberId AS bigint),
    NULL,
    NULL,
    CAST(w.lockId AS bigint),
    NULL,
    le.shares / 1e6,
    NULL,
    NULL,
    date_add('second', CAST(le.total_period AS bigint), le.lock_start_time),
    w.evt_tx_hash
  FROM nexusmutual_rwiv_ethereum.locks_evt_shareswithdrawn w
  LEFT JOIN lock_events le
    ON w.memberId = le.memberId
   AND w.lockId = le.lockId
   AND le.rn_desc = 1
  UNION ALL
  SELECT
    evt_block_time,
    evt_index,
    'Redemption',
    CAST(memberId AS bigint),
    memberAddress,
    CAST(requestId AS bigint),
    NULL,
    CAST(assets AS double) / 1e6,
    CAST(shares AS double) / 1e6,
    NULL,
    NULL,
    NULL,
    evt_tx_hash
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
)

SELECT
  e.block_time,
  e.event,
  e.member_id,
  COALESCE(e.emitted_address, ma.memberAddress) AS address,
  e.request_id,
  e.lock_id,
  e.amount_usdc,
  e.amount_rwiv,
  e.lock_top_up_rwiv,
  e.lock_period_added_days,
  e.lock_expiry,
  e.tx_hash
FROM events e
LEFT JOIN member_addresses ma ON e.member_id = CAST(ma.memberId AS bigint)
ORDER BY e.block_time DESC, e.evt_index DESC
