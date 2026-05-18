-- RWIV Q7 - Vault Balance vs Baseline Yield Cover (daily)
-- Dune query ID: 7382131
-- Daily snapshot comparing vault balance (rolled forward at the baseline yield
-- to the active cover's expiry) against the cover purchased to back the vault.
-- Also tracks cumulative NXM cover fees paid to date.
--
-- Sources:
--   - Vault balance time series + rate config from BQ1 (Dune 7392430). The
--     cover-expiry roll-up multiplies `cum_net_shares_raw` by the rate at
--     `cover_end_time`, which is why this query reads the rate-config columns
--     alongside the balance.
--   - Cover buy/edit txs from BQ2 (Dune 7506037, filtered to productId=425).
--     Each buyCover row preserves the cover's original premium / period / start
--     — query_3810247 retroactively truncates those on edits, which would
--     under-report Days Remaining and the Vault-vs-Cover comparison historically.
--
-- FIRST PASS SIMPLIFICATIONS:
--   1. Rate config lives in BQ1 and is hardcoded; baseline-yield rate edits
--      not handled there yet.
--   2. Assumes "Real World Insurance Vault" covers are USDC-denominated
--      (uses cover_amount_usdc directly as USDC amount).
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

rwiv_cover_buys AS (
  -- Every buyCover/editCover call for productId=425 (RWIV).
  -- Each row's fields are recorded at tx time and never rewritten by later edits.
  SELECT
    call_block_time                                  AS bought_at,
    call_block_time + period_days * INTERVAL '1' day AS cover_end,
    cover_amount_usdc,
    net_nxm_paid,
    net_usdc_paid
  FROM query_7506037
),

active_cover_per_day AS (
  -- Latest in-force buyCover for each day (each edit replaces the prior cover).
  -- A "cover_end <= as_of" check downstream distinguishes "still active" from
  -- "latest cover fully expired with no follow-up" (gap state → NULL columns).
  SELECT day, cover_end, cover_amount_usdc
  FROM (
    SELECT
      b.day,
      c.cover_end,
      c.cover_amount_usdc,
      ROW_NUMBER() OVER (PARTITION BY b.day ORDER BY c.bought_at DESC) AS rn
    FROM base b
    LEFT JOIN rwiv_cover_buys c ON c.bought_at <= b.as_of
  ) ranked
  WHERE rn = 1
),

cum_cover_fees AS (
  -- Cumulative net cover-fee outflow to date (sum of per-tx net_nxm_paid / net_usdc_paid).
  -- Net of refunds on edits, which is the actual cost of cover.
  SELECT
    b.day,
    COALESCE(SUM(c.net_nxm_paid),  0) AS cum_nxm,
    COALESCE(SUM(c.net_usdc_paid), 0) AS cum_usdc
  FROM base b
  LEFT JOIN rwiv_cover_buys c ON c.bought_at <= b.as_of
  GROUP BY b.day
)

SELECT
  b.day AS date,
  ROUND(b.vault_total_balance_usdc, 2) AS vault_total_balance_usdc,
  CASE
    WHEN ac.cover_end IS NULL OR ac.cover_end <= b.as_of THEN NULL
    ELSE ROUND(ac.cover_amount_usdc, 2)
  END AS cover_amount_usdc,
  CASE
    WHEN ac.cover_end IS NULL OR ac.cover_end <= b.as_of THEN NULL
    ELSE ac.cover_end
  END AS cover_expiry_time,
  CASE
    WHEN ac.cover_end IS NULL OR ac.cover_end <= b.as_of THEN NULL
    ELSE date_diff('day', b.as_of, ac.cover_end)
  END AS days_remaining,
  CASE
    WHEN ac.cover_end IS NULL OR ac.cover_end <= b.as_of THEN NULL
    ELSE ROUND(
      b.cum_net_shares_raw
        * b.start_rate
        * power(b.rate_per_second, date_diff('second', b.active_from, ac.cover_end))
        / 1e18 / 1e6,
      2
    )
  END AS vault_balance_at_cover_expiry_usdc,
  -- (Cover / Vault at expiry) - 1, expressed as a rate (positive = over-covered).
  CASE
    WHEN ac.cover_end IS NULL OR ac.cover_end <= b.as_of THEN NULL
    WHEN b.cum_net_shares_raw = 0 THEN NULL
    ELSE ROUND(
      ac.cover_amount_usdc / (
        b.cum_net_shares_raw
          * b.start_rate
          * power(b.rate_per_second, date_diff('second', b.active_from, ac.cover_end))
          / 1e18 / 1e6
      ) - 1,
      4
    )
  END AS pct_diff_cover_vs_vault_at_cover_end,
  ROUND(COALESCE(ccf.cum_nxm,  0), 2) AS cumulative_cover_fees_nxm,
  ROUND(COALESCE(ccf.cum_usdc, 0), 2) AS cumulative_cover_fees_usdc
FROM base b
LEFT JOIN active_cover_per_day ac  ON ac.day  = b.day
LEFT JOIN cum_cover_fees       ccf ON ccf.day = b.day
ORDER BY b.day DESC
