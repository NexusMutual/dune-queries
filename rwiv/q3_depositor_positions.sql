-- RWIV Q3 - Depositor Position Tracking
-- Dune query ID: 7364427
-- One row per memberId. Lifetime activity and current position.
--
-- FIRST PASS SIMPLIFICATIONS (deliberate, to be completed later):
--   1. No bonus/reward contribution. `Total Bonuses (USDC)` column from the
--      spec is omitted entirely. When `MemberRewarded` events first fire, add
--      that column as SUM(amount) per memberId and feed bonus cashflows into
--      TWRR.
--   2. No baseline-yield rate-edit handling. Rate config sourced from BQ0
--      (Dune 7392734) which is hardcoded as a single segment. When
--      `BaseRateChangeExecuted` first fires, replace BQ0 with a rate-history
--      chain — every consumer (Q1/Q3/Q4/Q4a/Q8/Q8a, plus BQ1 and downstream)
--      picks up the new rate automatically.
--   3. `total_apy` uses the TWRR/APY shape from the spec, but with no bonuses
--      and a constant rate every member's TWRR collapses to baseline APY
--      (~6%). Column name stays; replace expression with real per-segment
--      TWRR when sub-period returns start to differ.
--   4. Tracks positions by `memberId`, not address. Robust to member address
--      changes but does not model share-level ERC-20 transfers between
--      members (no such data exists yet).

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

member_deposits AS (
  SELECT
    memberId,
    SUM(CAST(assets AS double)) AS total_deposit_assets,
    SUM(CAST(shares AS double)) AS total_deposit_shares
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  GROUP BY memberId
),

member_redeems AS (
  SELECT
    memberId,
    SUM(CAST(assets AS double)) AS total_redeem_assets,
    SUM(CAST(shares AS double)) AS total_redeem_shares
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
  GROUP BY memberId
),

first_deposits AS (
  SELECT
    memberId,
    MIN(evt_block_time) AS first_deposit_time
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  GROUP BY memberId
),

positions AS (
  SELECT
    md.memberId,
    ma.memberAddress,
    fd.first_deposit_time,
    md.total_deposit_assets,
    md.total_deposit_shares,
    COALESCE(mr.total_redeem_assets, 0) AS total_redeem_assets,
    COALESCE(mr.total_redeem_shares, 0) AS total_redeem_shares,
    md.total_deposit_shares - COALESCE(mr.total_redeem_shares, 0) AS net_shares,
    cfg.start_rate,
    cfg.rate_per_second,
    cfg.active_from
  FROM member_deposits md
  CROSS JOIN rate_config cfg
  LEFT JOIN member_addresses ma ON md.memberId = ma.memberId
  LEFT JOIN member_redeems   mr ON md.memberId = mr.memberId
  LEFT JOIN first_deposits   fd ON md.memberId = fd.memberId
)

SELECT
  memberId AS member_id,
  memberAddress AS address,
  ROUND(net_shares / 1e6, 2) AS total_shares_rwiv,
  ROUND(
    net_shares
      * start_rate
      * power(rate_per_second, date_diff('second', active_from, NOW()))
      / 1e18 / 1e6,
    2
  ) AS current_balance_usdc,
  ROUND(total_deposit_assets / 1e6, 2) AS total_deposits_usdc,
  ROUND(total_redeem_assets / 1e6, 2) AS total_withdrawals_usdc,
  ROUND(
    (net_shares
      * start_rate
      * power(rate_per_second, date_diff('second', active_from, NOW()))
      / 1e18
     + total_redeem_assets
     - total_deposit_assets) / 1e6,
    2
  ) AS total_interest_earned_usdc,
  -- TWRR-shaped per-member; currently collapses to baseline APY
  -- because sub-period returns are identical (no bonuses, constant rate).
  ROUND(
    power(
      power(rate_per_second, date_diff('second', first_deposit_time, NOW())),
      31536000.0 / NULLIF(date_diff('second', first_deposit_time, NOW()), 0)
    ) - 1,
    4
  ) AS total_apy
FROM positions
WHERE net_shares > 0 OR total_deposit_assets > 0
ORDER BY current_balance_usdc DESC
