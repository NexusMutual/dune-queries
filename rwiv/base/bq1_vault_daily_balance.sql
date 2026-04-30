-- RWIV Base Query 1 - Vault Daily Balance
-- Dune query ID: 7392430
-- Daily net-shares and rolled-up vault balance time series.
-- Single source of truth for vault-wide balance state across Q6, Q7, and Q9.
--
-- FIRST PASS SIMPLIFICATIONS:
--   - Rate config hardcoded; baseline-yield rate edits not handled.
--     Will need updating on first BaseRateChangeExecuted event.
--
-- Output columns:
--   day                       - row's date label (date_trunc).
--   as_of                     - instant the row is valued at (end-of-day UTC,
--                               NOW() for today so the latest row reflects current state).
--   cum_net_shares_raw        - outstanding RWIV shares at as_of (no rate applied).
--                               Use this when applying the rate at a different timestamp
--                               (e.g. Q7's roll-up to cover expiry).
--   vault_total_balance_usdc  - cum_net_shares_raw rolled up at the baseline rate to as_of,
--                               in USDC (unrounded). Consumers display ROUND(_, 2).
--   start_rate, rate_per_second, active_from
--                             - rate config exposed so consumers can apply the rate
--                               at any timestamp without redefining the constants.

WITH rate_config AS (
  -- Single source of truth: Base Query 0 (Dune 7392734).
  SELECT start_rate, rate_per_second, active_from FROM query_7392734
),

first_activity AS (
  SELECT MIN(evt_block_time) AS first_time
  FROM (
    SELECT evt_block_time FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositrequested
    UNION ALL
    SELECT evt_block_time FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemrequested
  ) _
),

date_series AS (
  SELECT
    day,
    LEAST(day + INTERVAL '1' DAY, CAST(NOW() AS timestamp)) AS as_of
  FROM UNNEST(sequence(
    CAST(date_trunc('day', (SELECT first_time FROM first_activity)) AS timestamp),
    CAST(date_trunc('day', NOW()) AS timestamp),
    INTERVAL '1' DAY
  )) AS t(day)
),

cum_deposits AS (
  SELECT
    ds.day,
    COALESCE(SUM(CAST(df.shares AS double)), 0) AS cum_shares
  FROM date_series ds
  LEFT JOIN nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled df
    ON df.evt_block_time <= ds.as_of
  GROUP BY ds.day
),

cum_redeems AS (
  SELECT
    ds.day,
    COALESCE(SUM(CAST(rf.shares AS double)), 0) AS cum_shares
  FROM date_series ds
  LEFT JOIN nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled rf
    ON rf.evt_block_time <= ds.as_of
  GROUP BY ds.day
)

SELECT
  ds.day,
  ds.as_of,
  (cd.cum_shares - cr.cum_shares) AS cum_net_shares_raw,
  (cd.cum_shares - cr.cum_shares)
    * cfg.start_rate
    * power(cfg.rate_per_second, date_diff('second', cfg.active_from, ds.as_of))
    / 1e18 / 1e6
    AS vault_total_balance_usdc,
  cfg.start_rate,
  cfg.rate_per_second,
  cfg.active_from
FROM date_series ds
CROSS JOIN rate_config cfg
LEFT JOIN cum_deposits cd ON cd.day = ds.day
LEFT JOIN cum_redeems  cr ON cr.day = ds.day
ORDER BY ds.day DESC
