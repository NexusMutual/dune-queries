-- RWIV Q2a - Points Value & Projected Bonus Payouts
-- Dune query ID: 7989842
-- One row per member: season-1 bonus points, their current USDC value, the
-- projected season-end payout, and APY earned (baseline + bonus), now and at
-- season end. Q2 proper (retrospective distribution tracking from
-- MemberRewarded events) stays parked until the first payout happens.
--
-- Points (docs.nexusmutual.io/rwi-vault/yield-structure/bonuses/):
--   earning_rate = min(8, lock_period_days / 90) * rwiv_locked / 1000   [points/day]
--   points       = sum over lock segments of earning_rate * days
-- Each SharesLocked event opens a segment. SharesLocked emits shares as the
-- TOTAL after an edit but period as only the period ADDED (verified in the
-- Locks implementation source), and editLock never changes startTime, so the
-- lock's total period is the running sum of period. The docs' overlap rule
-- ("max earning rate") reduces to per-event segments because edits only ever
-- increase shares and period.
--
-- Season 1 ends 2026-09-30. Payout = 60% of NAV, if positive, pro-rata by points:
--   current pool   = 0.6 * max(latest NAV, 0)                (Q9, Dune 7396288)
--   projected pool = 0.6 * max(NAV projected to 30 Sep, 0)
--   NAV projection = latest NAV
--                    + daily Pendle PT appreciation * days remaining
--                    - RWIV market cap base-rate growth to 30 Sep
--                    - daily cover value decay * days remaining.
--   Daily PT appreciation = (PT par - PT value) / days to maturity, which is
--   exact and constant under Q9's linear lot-accrual model, and uses levels
--   rather than day-over-day diffs (Q9's latest row is a partial day).
--   Daily cover value decay = premium / period of the in-force cover (BQ2's
--   latest buyCover row, the same row Q9 values the cover from).
--
-- APY components:
--   baseline_apy = rate_per_second ^ 31536000 - 1            (BQ0, Dune 7392734)
--   bonus_apy    = points value / balance, annualized since first deposit
--   total_apy    = baseline_apy + bonus_apy
--
-- First pass assumptions:
--   - NAV projection drivers: PT accrual, market cap growth, cover value
--     decay. PT maturity 2026-12-10 hardcoded (update alongside Q9's
--     pt_tokens for new maturities; aggregate formula assumes a single
--     maturity). USDC/reUSDe holdings (currently 0) assumed flat.
--   - Cover renewals are assumed NAV-neutral on the day (cover value and
--     PFCFA rise by the same amount), with the value then decaying at the
--     in-force cover's daily rate. The rate reads BQ2's latest buyCover row,
--     so it auto-updates after each renewal.
--   - Locks as they stand: no future locks/edits assumed; accrual stops at lock
--     expiry (withdrawal is only possible after expiry, so no separate cap).
--   - Season-end balances assume no deposits/redemptions between now and 30 Sep.
--   - Single hardcoded rate segment via BQ0 (no BaseRateChangeExecuted yet).

WITH rate_config AS (
  -- Single source of truth: Base Query 0 (Dune 7392734).
  SELECT start_rate, rate_per_second, active_from FROM query_7392734
),

season AS (
  SELECT
    TIMESTAMP '2026-10-01 00:00:00 UTC' AS season_end,  -- accrual cutoff (end of 2026-09-30)
    TIMESTAMP '2026-12-10 00:00:00 UTC' AS pt_maturity  -- PT-reUSDe-10DEC2026 expiry, matches Q9 pt_tokens
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
    MIN(evt_block_time) AS first_deposit_time,
    SUM(CAST(shares AS double)) AS deposit_shares
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  GROUP BY memberId
),

member_redeems AS (
  SELECT
    memberId,
    SUM(CAST(shares AS double)) AS redeem_shares
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
  GROUP BY memberId
),

lock_events AS (
  -- One row per SharesLocked event. shares is the lock's TOTAL after the event;
  -- period is the running sum of emitted periods (edits emit only the period added).
  SELECT
    memberId,
    lockId,
    evt_block_time,
    evt_index,
    CAST(shares AS double) AS shares,
    SUM(CAST(period AS double)) OVER (
      PARTITION BY memberId, lockId ORDER BY evt_block_time, evt_index
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS period,
    MIN(evt_block_time)  OVER (PARTITION BY memberId, lockId) AS lock_start,
    LEAD(evt_block_time) OVER (PARTITION BY memberId, lockId ORDER BY evt_block_time, evt_index) AS next_evt_time
  FROM nexusmutual_rwiv_ethereum.locks_evt_shareslocked
),

lock_segments AS (
  -- One accrual segment per event state: runs from the event until the next
  -- edit, capped at lock expiry and season end. The min(8, days/90) multiplier
  -- uses the lock's TOTAL duration (original start -> current expiry), per docs.
  SELECT
    le.memberId,
    le.lockId,
    le.shares,
    le.evt_block_time AS seg_start,
    le.evt_index,
    le.next_evt_time,
    date_add('second', CAST(le.period AS integer), le.lock_start) AS state_expiry,
    LEAST(8.0, le.period / 86400.0 / 90.0) * (le.shares / 1e6) / 1000.0 AS points_per_day,
    s.season_end
  FROM lock_events le
  CROSS JOIN season s
),

segment_points AS (
  SELECT
    memberId,
    lockId,
    shares,
    state_expiry,
    points_per_day
      * GREATEST(date_diff('second', seg_start,
          LEAST(COALESCE(next_evt_time, NOW()), state_expiry, season_end)), 0) / 86400.0
      AS points_now,
    points_per_day
      * GREATEST(date_diff('second', seg_start,
          LEAST(COALESCE(next_evt_time, season_end), state_expiry, season_end)), 0) / 86400.0
      AS points_season_end
  FROM lock_segments
),

member_points AS (
  SELECT
    memberId,
    SUM(points_now) AS points_now,
    SUM(points_season_end) AS points_season_end
  FROM segment_points
  GROUP BY memberId
),

active_locked AS (
  -- Currently locked shares: latest state per lock, unexpired, not withdrawn.
  SELECT
    ls.memberId,
    SUM(ls.shares) AS locked_shares
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY memberId, lockId ORDER BY seg_start DESC, evt_index DESC) AS rn
    FROM lock_segments
  ) ls
  LEFT JOIN nexusmutual_rwiv_ethereum.locks_evt_shareswithdrawn w
    ON w.memberId = ls.memberId AND w.lockId = ls.lockId
  WHERE ls.rn = 1
    AND ls.state_expiry > NOW()
    AND w.memberId IS NULL
  GROUP BY ls.memberId
),

nav_latest AS (
  -- Latest daily NAV row from Q9 (Dune 7396288). The latest row's as_of is
  -- the execution time (a partial day), so the projection works from levels,
  -- never day-over-day diffs.
  SELECT
    nav_usdc,
    reusde_pt_amount,
    reusde_pt_value_usde,
    total_market_cap_rwiv_usdc
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (ORDER BY "date" DESC) AS rn
    FROM query_7396288
  )
  WHERE rn = 1
),

cover_decay AS (
  -- Daily amortization of the in-force cover: premium / period of the latest
  -- buyCover row in BQ2 (Dune 7506037), the same row Q9 values the cover
  -- from. Auto-updates on renewal; 0 if no cover was ever bought.
  SELECT
    COALESCE(MAX_BY(premium_usdc / period_days, call_block_time), 0) AS cover_decay_per_day
  FROM query_7506037
),

nav_projection AS (
  -- NAV projected to season end = latest NAV
  --   + daily PT appreciation x days remaining
  --   - RWIV market cap growth at the base rate (compounded to season end)
  --   - daily cover value decay x days remaining.
  -- Daily PT appreciation = (par - value) / days to maturity: exact and
  -- constant under Q9's linear lot-accrual model, robust to top-ups/sales.
  -- All terms degrade to 0 once the season is over.
  SELECT
    n.nav_usdc AS nav_latest,
    n.nav_usdc
      + (n.reusde_pt_amount - n.reusde_pt_value_usde)
        / NULLIF(date_diff('second', NOW(), s.pt_maturity) / 86400.0, 0)
        * (GREATEST(date_diff('second', NOW(), s.season_end), 0) / 86400.0)
      - n.total_market_cap_rwiv_usdc
        * (power(cfg.rate_per_second, GREATEST(date_diff('second', NOW(), s.season_end), 0)) - 1)
      - cd.cover_decay_per_day
        * (GREATEST(date_diff('second', NOW(), s.season_end), 0) / 86400.0)
      AS nav_projected
  FROM nav_latest n
  CROSS JOIN season s
  CROSS JOIN rate_config cfg
  CROSS JOIN cover_decay cd
),

pools AS (
  SELECT
    0.6 * GREATEST(nav_latest, 0) AS current_pool_usdc,
    0.6 * GREATEST(nav_projected, 0) AS projected_pool_usdc
  FROM nav_projection
),

points_totals AS (
  SELECT
    SUM(points_now) AS total_points_now,
    SUM(points_season_end) AS total_points_season_end
  FROM member_points
),

member_rows AS (
  SELECT
    md.memberId,
    ma.memberAddress,
    md.first_deposit_time,
    md.deposit_shares - COALESCE(mr.redeem_shares, 0) AS net_shares,
    COALESCE(al.locked_shares, 0) AS locked_shares,
    COALESCE(mp.points_now, 0) AS points_now,
    COALESCE(mp.points_season_end, 0) AS points_season_end
  FROM member_deposits md
  LEFT JOIN member_addresses ma ON md.memberId = ma.memberId
  LEFT JOIN member_redeems   mr ON md.memberId = mr.memberId
  LEFT JOIN active_locked    al ON md.memberId = al.memberId
  LEFT JOIN member_points    mp ON md.memberId = mp.memberId
),

enriched AS (
  SELECT
    m.memberId,
    m.memberAddress,
    m.first_deposit_time,
    m.locked_shares,
    m.points_now,
    m.points_season_end,
    s.season_end,
    p.current_pool_usdc,
    p.projected_pool_usdc,
    t.total_points_now,
    t.total_points_season_end,
    -- balances via the rate roll-up (Q3/Q8 pattern)
    m.net_shares
      * cfg.start_rate
      * power(cfg.rate_per_second, date_diff('second', cfg.active_from, NOW()))
      / 1e18 / 1e6 AS balance_now_usdc,
    m.net_shares
      * cfg.start_rate
      * power(cfg.rate_per_second, date_diff('second', cfg.active_from, s.season_end))
      / 1e18 / 1e6 AS balance_season_end_usdc,
    m.points_now / NULLIF(t.total_points_now, 0) * p.current_pool_usdc AS points_value_now_usdc,
    m.points_season_end / NULLIF(t.total_points_season_end, 0) * p.projected_pool_usdc AS projected_payout_usdc,
    power(cfg.rate_per_second, 31536000) - 1 AS baseline_apy
  FROM member_rows m
  CROSS JOIN rate_config cfg
  CROSS JOIN season s
  CROSS JOIN pools p
  CROSS JOIN points_totals t
),

apy AS (
  SELECT
    *,
    (points_value_now_usdc / NULLIF(balance_now_usdc, 0))
      * (31536000.0 / NULLIF(date_diff('second', first_deposit_time, NOW()), 0)) AS current_bonus_apy,
    (projected_payout_usdc / NULLIF(balance_season_end_usdc, 0))
      * (31536000.0 / NULLIF(date_diff('second', first_deposit_time, season_end), 0)) AS projected_bonus_apy
  FROM enriched
)

SELECT
  memberId AS member_id,
  memberAddress AS address,
  ROUND(locked_shares / 1e6, 2) AS rwiv_locked,
  ROUND(balance_now_usdc, 2) AS current_balance_usdc,
  ROUND(points_now, 2) AS current_points,
  ROUND(100 * points_now / NULLIF(total_points_now, 0), 2) AS current_points_share_pct,
  ROUND(points_value_now_usdc, 2) AS current_points_value_usdc,
  ROUND(points_season_end, 2) AS projected_season_end_points,
  ROUND(100 * points_season_end / NULLIF(total_points_season_end, 0), 2) AS projected_points_share_pct,
  ROUND(projected_payout_usdc, 2) AS projected_payout_usdc,
  ROUND(baseline_apy, 4) AS baseline_apy,
  ROUND(current_bonus_apy, 4) AS current_bonus_apy,
  ROUND(baseline_apy + current_bonus_apy, 4) AS current_total_apy,
  ROUND(projected_bonus_apy, 4) AS projected_bonus_apy,
  ROUND(baseline_apy + projected_bonus_apy, 4) AS projected_total_apy,
  ROUND(current_pool_usdc, 2) AS current_bonus_pool_usdc,
  ROUND(projected_pool_usdc, 2) AS projected_bonus_pool_usdc
FROM apy
ORDER BY current_points DESC
