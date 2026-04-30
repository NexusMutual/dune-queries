-- RWIV Q7 - Vault Balance vs Baseline Yield Cover (daily)
-- Dune query ID: 7382131
-- Daily snapshot comparing vault balance (rolled forward at the baseline yield
-- to the active cover's expiry) against the cover purchased to back the vault.
-- Also tracks cumulative NXM cover fees paid to date.
--
-- Sources `vault_total_balance_usdc`, `cum_net_shares_raw`, daily date series,
-- and rate config from Base Query 1 (Dune 7392430). The cover-expiry roll-up
-- multiplies `cum_net_shares_raw` by the rate at `cover_end_time`, which is
-- why this query reads the rate-config columns alongside the balance.
--
-- FIRST PASS SIMPLIFICATIONS:
--   1. Rate config lives in BQ1 and is hardcoded; baseline-yield rate edits
--      not handled there yet.
--   2. Assumes "Real World Insurance Vault" covers are USDC-denominated
--      (uses native_cover_amount directly as USDC amount).
--
-- "No active cover" state on a given date emits NULL across the cover columns.

WITH base AS (
  SELECT
    day,
    as_of,
    cum_net_shares_raw,
    vault_total_balance_usdc,
    start_rate,
    rate_per_second,
    active_from
  FROM query_7392430
),

rwiv_covers AS (
  SELECT
    cover_id,
    cover_start_time,
    cover_end_time,
    native_cover_amount AS cover_amount,
    cover_asset,
    premium_nxm
  FROM query_3810247
  WHERE product_name = 'Real World Insurance Vault'
),

-- One active cover per day (defensive ROW_NUMBER tie-break: latest start wins
-- if intervals ever overlap; per spec they should be discrete).
active_cover_per_day_ranked AS (
  SELECT
    b.day,
    rc.cover_id,
    rc.cover_end_time,
    rc.cover_amount,
    ROW_NUMBER() OVER (
      PARTITION BY b.day
      ORDER BY rc.cover_start_time DESC
    ) AS rn
  FROM base b
  LEFT JOIN rwiv_covers rc
    ON rc.cover_start_time <= b.as_of
   AND rc.cover_end_time   >  b.as_of
),

active_cover_per_day AS (
  SELECT day, cover_id, cover_end_time, cover_amount
  FROM active_cover_per_day_ranked
  WHERE rn = 1
),

cum_cover_fees AS (
  SELECT
    b.day,
    COALESCE(SUM(rc.premium_nxm), 0) AS cum_nxm
  FROM base b
  LEFT JOIN rwiv_covers rc
    ON rc.cover_start_time <= b.as_of
  GROUP BY b.day
)

SELECT
  b.day AS date,
  ROUND(b.vault_total_balance_usdc, 2) AS vault_total_balance_usdc,
  ac.cover_end_time AS cover_expiry_time,
  CASE
    WHEN ac.cover_end_time IS NULL THEN NULL
    ELSE date_diff('day', b.as_of, ac.cover_end_time)
  END AS days_remaining,
  CASE
    WHEN ac.cover_end_time IS NULL THEN NULL
    ELSE ROUND(
      b.cum_net_shares_raw
        * b.start_rate
        * power(b.rate_per_second, date_diff('second', b.active_from, ac.cover_end_time))
        / 1e18 / 1e6,
      2
    )
  END AS vault_balance_at_cover_expiry_usdc,
  CASE
    WHEN ac.cover_amount IS NULL THEN NULL
    ELSE ROUND(ac.cover_amount, 2)
  END AS cover_amount_usdc,
  -- (Cover / Vault at expiry) - 1, expressed as a rate (positive = over-covered).
  CASE
    WHEN ac.cover_amount IS NULL THEN NULL
    WHEN b.cum_net_shares_raw = 0 THEN NULL
    ELSE ROUND(
      ac.cover_amount / (
        b.cum_net_shares_raw
          * b.start_rate
          * power(b.rate_per_second, date_diff('second', b.active_from, ac.cover_end_time))
          / 1e18 / 1e6
      ) - 1,
      4
    )
  END AS pct_diff_cover_vs_vault_at_cover_end,
  ROUND(COALESCE(ccf.cum_nxm, 0), 2) AS cumulative_cover_fees_nxm
FROM base b
LEFT JOIN active_cover_per_day ac  ON ac.day  = b.day
LEFT JOIN cum_cover_fees       ccf ON ccf.day = b.day
ORDER BY b.day DESC
