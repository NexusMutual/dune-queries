-- RWIV Q8a - Locks Unlock Schedule (Drop-off Chart)
-- Dune query ID: 7363191
-- Time series of total RWIV (and projected USDC) still locked at each future date.
-- Declines to zero as active locks expire. Inspired by the Cover expiry drop-off viz.
-- Only includes locks that are currently active (not withdrawn, not yet expired).
-- Also projects total vault balance forward (latest cum_net_shares_raw from B1, rolled
-- up at the baseline rate) for comparison against the locked portion.

WITH rate_config AS (
  -- Single source of truth: Base Query 0 (Dune 7392734).
  SELECT start_rate, rate_per_second, active_from FROM query_7392734
),

latest_vault_state AS (
  -- Latest outstanding RWIV shares from Base Query 1 (Dune 7392430).
  -- Projected forward using the same rate formula as usdc_value_remaining_locked.
  SELECT cum_net_shares_raw
  FROM query_7392430
  ORDER BY day DESC
  LIMIT 1
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
  SELECT DISTINCT memberId, lockId
  FROM nexusmutual_rwiv_ethereum.locks_evt_shareswithdrawn
),

active_locks AS (
  SELECT
    ls.shares,
    date_add('second', CAST(ls.latest_period AS integer), ls.original_lock_timestamp) AS latest_expiry
  FROM lock_state ls
  LEFT JOIN withdrawals w ON ls.memberId = w.memberId AND ls.lockId = w.lockId
  WHERE w.memberId IS NULL
    AND date_diff('second', ls.original_lock_timestamp, NOW()) < ls.latest_period
),

date_series AS (
  SELECT day
  FROM UNNEST(
    sequence(
      CAST(date_trunc('day', NOW()) AS timestamp),
      CAST(date_trunc('day', NOW()) AS timestamp) + INTERVAL '732' DAY,
      INTERVAL '1' DAY
    )
  ) AS t(day)
),

daily_totals AS (
  SELECT
    d.day,
    SUM(CASE WHEN al.latest_expiry > d.day THEN al.shares ELSE 0 END) AS rwiv_locked_raw,
    SUM(
      CASE WHEN al.latest_expiry > d.day
        THEN al.shares
          * cfg.start_rate
          * power(cfg.rate_per_second, date_diff('second', cfg.active_from, d.day))
          / 1e18
        ELSE 0
      END
    ) AS usdc_locked_raw,
    MAX(
      lvs.cum_net_shares_raw
        * cfg.start_rate
        * power(cfg.rate_per_second, date_diff('second', cfg.active_from, d.day))
        / 1e18
    ) AS vault_total_raw
  FROM date_series d
  CROSS JOIN rate_config cfg
  CROSS JOIN latest_vault_state lvs
  CROSS JOIN active_locks al
  GROUP BY d.day
  HAVING SUM(CASE WHEN al.latest_expiry > d.day THEN al.shares ELSE 0 END) > 0
)

SELECT
  day AS date,
  ROUND(rwiv_locked_raw / 1e6, 2) AS rwiv_remaining_locked,
  ROUND(usdc_locked_raw / 1e6, 2) AS usdc_value_remaining_locked,
  ROUND(vault_total_raw / 1e6, 2) AS total_vault_balance_usdc,
  ROUND(100.0 * usdc_locked_raw / vault_total_raw, 2) AS pct_locked
FROM daily_totals
ORDER BY day ASC
