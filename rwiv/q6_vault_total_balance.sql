-- RWIV Q6 - Vault Total Balance (daily)
-- Dune query ID: 7364527
-- Daily snapshot of vault-wide state, from the first on-chain request through today.
-- Each row reflects end-of-day UTC state (i.e. includes any event with evt_block_time < day + 1).
--
-- Sources `vault_total_balance_usdc`, daily date series (`day`, `as_of`),
-- and rate config from Base Query 1 (Dune 7392430). Q6 layers on its own
-- vault-cap, deposit/redeem-asset cumulatives, pending requests, and APY.
--
-- FIRST PASS SIMPLIFICATIONS (same deferrals as Q3; see project_q3_simplifications memory):
--   1. No `total_bonuses_usdc` column. Add when `MemberRewarded` first fires.
--   2. No baseline-yield rate-edit handling (rate config lives in BQ1).
--   3. `total_apy` is TWRR-shaped but collapses to baseline APY (~6%) because
--      sub-period returns are identical in the current regime. Replace with real
--      per-segment TWRR when bonuses or rate changes exist.
--   4. Initial vault cap hardcoded at 10M USDC (contract constant; no event emits it).
--      Only subsequent changes via `setAssetCap` are reflected from their call time.

WITH
base AS (
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

initial_cap AS (
  SELECT CAST(10000000e6 AS double) AS initial_asset_cap
),

cap_changes AS (
  SELECT call_block_time AS change_time, CAST(newAssetCap AS double) AS new_cap
  FROM nexusmutual_rwiv_ethereum.rwivault_call_setassetcap
  WHERE call_success = true
),

cap_per_day_ranked AS (
  SELECT
    b.day,
    COALESCE(cc.new_cap, ic.initial_asset_cap) AS cap_usdc,
    ROW_NUMBER() OVER (
      PARTITION BY b.day
      ORDER BY cc.change_time DESC NULLS LAST
    ) AS rn
  FROM base b
  CROSS JOIN initial_cap ic
  LEFT JOIN cap_changes cc
    ON cc.change_time <= b.as_of
),

cap_per_day AS (
  SELECT day, cap_usdc FROM cap_per_day_ranked WHERE rn = 1
),

cum_deposit_assets AS (
  SELECT
    b.day,
    COALESCE(SUM(CAST(df.assets AS double)), 0) AS cum_assets
  FROM base b
  LEFT JOIN nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled df
    ON df.evt_block_time <= b.as_of
  GROUP BY b.day
),

cum_redeem_assets AS (
  SELECT
    b.day,
    COALESCE(SUM(CAST(rf.assets AS double)), 0) AS cum_assets
  FROM base b
  LEFT JOIN nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled rf
    ON rf.evt_block_time <= b.as_of
  GROUP BY b.day
),

deposit_requests AS (
  SELECT requestId, evt_block_time AS request_time, CAST(assets AS double) AS requested_assets
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositrequested
),

deposit_fill_by_request_day AS (
  SELECT
    b.day,
    df.requestId,
    SUM(CAST(df.assets AS double)) AS cum_filled
  FROM base b
  JOIN nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled df
    ON df.evt_block_time <= b.as_of
  GROUP BY b.day, df.requestId
),

pending_deposits AS (
  SELECT
    b.day,
    COALESCE(SUM(
      CASE
        WHEN dr.request_time > b.as_of THEN 0
        WHEN dc.evt_block_time IS NOT NULL
          AND dc.evt_block_time <= b.as_of THEN 0
        ELSE GREATEST(0, dr.requested_assets - COALESCE(dfr.cum_filled, 0))
      END
    ), 0) AS pending_assets
  FROM base b
  CROSS JOIN deposit_requests dr
  LEFT JOIN nexusmutual_rwiv_ethereum.rwivault_evt_depositrequestcanceled dc
    ON dc.requestId = dr.requestId
  LEFT JOIN deposit_fill_by_request_day dfr
    ON dfr.day = b.day AND dfr.requestId = dr.requestId
  GROUP BY b.day
),

redeem_requests AS (
  SELECT requestId, evt_block_time AS request_time, CAST(shares AS double) AS requested_shares
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemrequested
),

redeem_fill_by_request_day AS (
  SELECT
    b.day,
    rf.requestId,
    SUM(CAST(rf.shares AS double)) AS cum_filled_shares
  FROM base b
  JOIN nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled rf
    ON rf.evt_block_time <= b.as_of
  GROUP BY b.day, rf.requestId
),

pending_redeems AS (
  SELECT
    b.day,
    COALESCE(SUM(
      CASE
        WHEN rr.request_time > b.as_of THEN 0
        WHEN rc.evt_block_time IS NOT NULL
          AND rc.evt_block_time <= b.as_of THEN 0
        ELSE GREATEST(0, rr.requested_shares - COALESCE(rfr.cum_filled_shares, 0))
      END
    ), 0) AS pending_shares
  FROM base b
  CROSS JOIN redeem_requests rr
  LEFT JOIN nexusmutual_rwiv_ethereum.rwivault_evt_redeemrequestcanceled rc
    ON rc.requestId = rr.requestId
  LEFT JOIN redeem_fill_by_request_day rfr
    ON rfr.day = b.day AND rfr.requestId = rr.requestId
  GROUP BY b.day
),

vault_first_deposit AS (
  SELECT MIN(evt_block_time) AS first_deposit_time
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
)

SELECT
  b.day AS date,
  ROUND(cp.cap_usdc / 1e6, 2) AS vault_cap_usdc,
  ROUND(b.vault_total_balance_usdc, 2) AS vault_total_balance_usdc,
  ROUND(
    -pr.pending_shares
      * b.start_rate
      * power(b.rate_per_second, date_diff('second', b.active_from, b.as_of))
      / 1e18 / 1e6,
    2
  ) AS total_pending_withdrawals_usdc,
  ROUND(pd.pending_assets / 1e6, 2) AS total_pending_deposits_usdc,
  ROUND(cda.cum_assets / 1e6, 2) AS total_deposits_usdc,
  ROUND(-cra.cum_assets / 1e6, 2) AS total_filled_withdrawals_usdc,
  ROUND(
    b.vault_total_balance_usdc + (cra.cum_assets - cda.cum_assets) / 1e6,
    2
  ) AS total_vault_interest_earned_usdc,
  -- TWRR-shaped vault-level APY; currently collapses to baseline APY
  -- (~0.06) until bonuses or rate edits appear.
  ROUND(
    CASE
      WHEN vfd.first_deposit_time IS NULL THEN NULL
      WHEN b.as_of < vfd.first_deposit_time THEN NULL
      WHEN date_diff('second', vfd.first_deposit_time, b.as_of) = 0 THEN NULL
      ELSE power(
        power(b.rate_per_second, date_diff('second', vfd.first_deposit_time, b.as_of)),
        31536000.0 / date_diff('second', vfd.first_deposit_time, b.as_of)
      ) - 1
    END,
    4
  ) AS total_apy
FROM base b
CROSS JOIN vault_first_deposit vfd
LEFT JOIN cap_per_day        cp  ON cp.day  = b.day
LEFT JOIN cum_deposit_assets cda ON cda.day = b.day
LEFT JOIN cum_redeem_assets  cra ON cra.day = b.day
LEFT JOIN pending_deposits   pd  ON pd.day  = b.day
LEFT JOIN pending_redeems    pr  ON pr.day  = b.day
ORDER BY b.day DESC
