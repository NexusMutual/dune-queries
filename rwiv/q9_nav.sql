-- RWIV Q9 - NAV (daily)
-- Dune query ID: 7396288
-- Daily Vault Net Asset Value snapshot.
--   Asset side:     USDC + reUSDe + Pendle PT-reUSDe balances at the VO multisig
--                   + un-amortised cover value.
--   Liability side: outstanding RWIV market cap + Pre-funded Cover Fee Asset.
--   NAV =           Total Assets − Total Liabilities.
--
-- Sources:
--   - Vault Total Balance + date series from BQ1 (Dune 7392430).
--   - VO Multisig:                       0x09f0fb4405e4445849519511a407e68f697d1822
--   - USDC token:                        0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
--   - reUSDe token (18 decimals):        0xddc0f880ff6e4e22e4b74632fbb43ce4df6ccc5a
--   - USDe token:                        0x4c9edd5852cd905f086c759e8383e09bff1e68b3
--   - reUSDe SharePriceCalculator:       0x1262a408de54db9ae3fb3bb0e429c319fbee9915
--     SharePriceSet topic0:              0x4a1ac874d2c95dbf06e8751911760755154ef5c49e9ecfd9b5e71a11664a239c
--   - PT-reUSDe-10DEC2026 (18 decimals): 0x2ae4f59e500b6ddeb88c480edf277eda54a00205
--     Pendle Principal Token; maturity 2026-12-10 00:00 UTC (on-chain expiry() = 1796860800).
--   - Cover buy/edit txs (per buyCover call): query_7506037 (filtered to productId = 425).
--     Used for both Value of Cover (latest in-force buyCover) and PFCFA (sum of net_usdc_paid).
--
-- Out of scope (per spec, first pass):
--   - NXM holdings of the multisig (separate "grant" workstream).
--   - Pending Claims (asset side).
--   - Pending Bonus Distributions (liability side).
--
-- First pass assumptions:
--   - Date series starts at first vault deposit (via BQ1). Re's NAV oracle
--     coverage predates the vault launch, so no fallback default is needed.
--   - reUSDe underlying = USDe, and USDe is treated 1:1 with USDC.
--   - PFCFA per-fee step function (per docs). Funding period: 2026-04-24
--     (first cover) through 2027-12-31. Release symmetric around 2027-12-31:
--         release_day = 2 * DATE '2027-12-31' - cover_buy_day
--     A fee is in PFCFA on its release_day and out the day after.
--   - Pendle PT positions are marked to model, not market: each acquisition lot
--     accrues linearly in time from its USDe cost basis (reUSDe sent in the same
--     tx × reUSDe NAV + USDC sent 1:1) to par (1 USDe per PT) at maturity, then
--     holds at par until redeemed. Position value = PT balance × blended
--     average-cost unit value, so top-ups, partial sales and redemption
--     self-correct. A new PT token (new maturity) needs one row in pt_tokens.

WITH base AS (
  SELECT day, as_of, vault_total_balance_usdc
  FROM query_7392430
),

usdc_balance AS (
  SELECT
    b.day,
    COALESCE(SUM(
      CASE
        WHEN t."to"   = 0x09f0fb4405e4445849519511a407e68f697d1822 THEN  CAST(t.value AS double)
        WHEN t."from" = 0x09f0fb4405e4445849519511a407e68f697d1822 THEN -CAST(t.value AS double)
      END
    ), 0) / 1e6 AS usdc_amount
  FROM base b
  LEFT JOIN erc20_ethereum.evt_transfer t
    ON t.contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
   AND t.evt_block_date >= DATE '2026-03-01'  -- vault first activity 2026-03-26
   AND (t."to" = 0x09f0fb4405e4445849519511a407e68f697d1822
     OR t."from" = 0x09f0fb4405e4445849519511a407e68f697d1822)
   AND t.evt_block_time <= b.as_of
  GROUP BY b.day
),

reusde_balance AS (
  SELECT
    b.day,
    COALESCE(SUM(
      CASE
        WHEN t."to"   = 0x09f0fb4405e4445849519511a407e68f697d1822 THEN  CAST(t.value AS double)
        WHEN t."from" = 0x09f0fb4405e4445849519511a407e68f697d1822 THEN -CAST(t.value AS double)
      END
    ), 0) / 1e18 AS reusde_amount
  FROM base b
  LEFT JOIN erc20_ethereum.evt_transfer t
    ON t.contract_address = 0xddc0f880ff6e4e22e4b74632fbb43ce4df6ccc5a
   AND t.evt_block_date >= DATE '2026-03-01'  -- vault first activity 2026-03-26
   AND (t."to" = 0x09f0fb4405e4445849519511a407e68f697d1822
     OR t."from" = 0x09f0fb4405e4445849519511a407e68f697d1822)
   AND t.evt_block_time <= b.as_of
  GROUP BY b.day
),

reusde_nav_events AS (
  SELECT
    block_time AS evt_block_time,
    CAST(varbinary_to_uint256(varbinary_substring(data, 33, 32)) AS double) / 1e18
      AS new_price_usde_per_share
  FROM ethereum.logs
  WHERE contract_address = 0x1262a408de54db9ae3fb3bb0e429c319fbee9915
    AND topic0 = 0x4a1ac874d2c95dbf06e8751911760755154ef5c49e9ecfd9b5e71a11664a239c
    AND block_date >= DATE '2026-03-01'  -- partition pruning; daily NAV events give buffer before vault first activity (2026-03-26)
),

reusde_nav_per_day AS (
  SELECT
    b.day,
    MAX_BY(e.new_price_usde_per_share, e.evt_block_time) AS reusde_nav_usde
  FROM base b
  LEFT JOIN reusde_nav_events e ON e.evt_block_time <= b.as_of
  GROUP BY b.day
),

pt_tokens (pt_address, maturity) AS (
  -- Pendle PT positions held by the VO Multisig: one row per PT token.
  -- maturity = the PT's on-chain expiry(); 1 PT redeems for 1 USDe worth of
  -- reUSDe at/after it. A new PT position (new maturity) = add one row here.
  -- Top-ups, partial sales and redemptions of listed tokens need no change.
  VALUES
    (0x2ae4f59e500b6ddeb88c480edf277eda54a00205, TIMESTAMP '2026-12-10 00:00:00')  -- PT-reUSDe-10DEC2026
),

pt_transfers AS (
  -- PT flows in/out of the VO Multisig, signed from the multisig's perspective.
  SELECT
    t.contract_address AS pt_address,
    t.evt_tx_hash,
    t.evt_block_time,
    CASE
      WHEN t."to"   = 0x09f0fb4405e4445849519511a407e68f697d1822 THEN  CAST(t.value AS double)
      WHEN t."from" = 0x09f0fb4405e4445849519511a407e68f697d1822 THEN -CAST(t.value AS double)
    END / 1e18 AS pt_flow
  FROM erc20_ethereum.evt_transfer t
  JOIN pt_tokens pt ON pt.pt_address = t.contract_address
  WHERE t.evt_block_date >= DATE '2026-03-01'  -- vault first activity 2026-03-26
    AND (t."to" = 0x09f0fb4405e4445849519511a407e68f697d1822
      OR t."from" = 0x09f0fb4405e4445849519511a407e68f697d1822)
),

pt_funding_flows AS (
  -- reUSDe / USDC sent by the multisig per tx — the funding legs of PT acquisitions.
  SELECT
    t.evt_tx_hash,
    SUM(CASE WHEN t.contract_address = 0xddc0f880ff6e4e22e4b74632fbb43ce4df6ccc5a
             THEN CAST(t.value AS double) / 1e18 ELSE 0 END) AS reusde_out,
    SUM(CASE WHEN t.contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
             THEN CAST(t.value AS double) / 1e6 ELSE 0 END) AS usdc_out
  FROM erc20_ethereum.evt_transfer t
  WHERE t."from" = 0x09f0fb4405e4445849519511a407e68f697d1822
    AND t.contract_address IN (0xddc0f880ff6e4e22e4b74632fbb43ce4df6ccc5a,
                               0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48)
    AND t.evt_block_date >= DATE '2026-03-01'  -- vault first activity 2026-03-26
  GROUP BY t.evt_tx_hash
),

pt_lots AS (
  -- One lot per tx that net-increased the multisig's PT balance.
  -- Cost basis (USDe) = reUSDe sent in the same tx × reUSDe NAV at acquisition
  --                   + USDC sent in the same tx (1:1 with USDe).
  -- Each lot accrues linearly from cost_usde to par (pt_amount × 1 USDe) at maturity.
  SELECT
    a.pt_address,
    a.maturity,
    a.acq_time,
    a.pt_amount,
    COALESCE(a.reusde_out, 0) * COALESCE(MAX_BY(e.new_price_usde_per_share, e.evt_block_time), 0)
      + COALESCE(a.usdc_out, 0) AS cost_usde
  FROM (
    SELECT
      p.pt_address,
      pt.maturity,
      p.evt_tx_hash,
      MIN(p.evt_block_time) AS acq_time,
      SUM(p.pt_flow) AS pt_amount,
      MAX(ff.reusde_out) AS reusde_out,
      MAX(ff.usdc_out) AS usdc_out
    FROM pt_transfers p
    JOIN pt_tokens pt ON pt.pt_address = p.pt_address
    LEFT JOIN pt_funding_flows ff ON ff.evt_tx_hash = p.evt_tx_hash
    GROUP BY p.pt_address, pt.maturity, p.evt_tx_hash
    HAVING SUM(p.pt_flow) > 0
  ) a
  LEFT JOIN reusde_nav_events e ON e.evt_block_time <= a.acq_time
  GROUP BY a.pt_address, a.maturity, a.acq_time, a.pt_amount, a.reusde_out, a.usdc_out
),

pt_lot_values AS (
  -- Accrued USDe value of all lots acquired on or before each day's as_of.
  -- Per lot: cost_usde + (pt_amount − cost_usde) × elapsed / (acq → maturity), capped at par.
  SELECT
    b.day,
    l.pt_address,
    SUM(l.pt_amount) AS pt_acquired,
    SUM(
      l.cost_usde + (l.pt_amount - l.cost_usde)
        * LEAST(1, date_diff('second', l.acq_time, b.as_of)
                   / CAST(date_diff('second', l.acq_time, l.maturity) AS double))
    ) AS lots_value_usde
  FROM base b
  JOIN pt_lots l ON l.acq_time <= b.as_of
  GROUP BY b.day, l.pt_address
),

pt_value_per_day AS (
  -- PT balance × blended (average-cost) unit value, summed across PT tokens.
  -- Sales/redemptions shrink the balance and scale the value down pro-rata.
  SELECT
    b.day,
    COALESCE(SUM(pb.pt_balance), 0) AS pt_amount,
    COALESCE(SUM(pb.pt_balance * lv.lots_value_usde / lv.pt_acquired), 0) AS pt_value_usde
  FROM base b
  LEFT JOIN (
    SELECT
      b2.day,
      p.pt_address,
      SUM(p.pt_flow) AS pt_balance
    FROM base b2
    JOIN pt_transfers p ON p.evt_block_time <= b2.as_of
    GROUP BY b2.day, p.pt_address
  ) pb ON pb.day = b.day
  LEFT JOIN pt_lot_values lv ON lv.day = pb.day AND lv.pt_address = pb.pt_address
  GROUP BY b.day
),

rwiv_cover_buys AS (
  -- Every buyCover/editCover call by the VO Multisig for productId = 425.
  -- Each row's fields are recorded at tx time and never rewritten by later edits.
  --   premium_usdc    - full premium of the cover purchased in this tx (gross)
  --   net_usdc_paid   - owner's actual outflow this tx (new premium - refund of prior cover, if edit)
  --   period_days     - cover duration parameter as set at buy time
  --   bought_at       - call_block_time; the moment this cover becomes in-force
  --   cover_end       - bought_at + period_days days; expiry if not edited
  SELECT
    call_block_time                                 AS bought_at,
    call_block_time + period_days * INTERVAL '1' day AS cover_end,
    period_days,
    premium_usdc,
    net_usdc_paid
  FROM query_7506037
),

value_of_cover_per_day AS (
  -- Un-amortised value of the IN-FORCE cover at each end-of-day UTC.
  -- The in-force cover is the latest buyCover with bought_at <= as_of.
  -- Each edit replaces the prior cover (refund + new purchase), so the latest
  -- buyCover row IS the in-force cover. We use its original premium_usdc /
  -- period_days / bought_at — query_3810247 retroactively truncates those on
  -- edits, which would massively under-report the historical Value of Cover.
  SELECT
    day,
    CASE
      WHEN bought_at IS NULL  THEN NULL  -- no cover ever bought
      WHEN cover_end <= as_of THEN 0     -- in-force cover fully expired with no follow-up
      ELSE premium_usdc * date_diff('day', as_of, cover_end) / period_days
    END AS value_of_cover_usdc
  FROM (
    SELECT
      b.day,
      b.as_of,
      c.bought_at,
      c.cover_end,
      c.period_days,
      c.premium_usdc,
      ROW_NUMBER() OVER (PARTITION BY b.day ORDER BY c.bought_at DESC) AS rn
    FROM base b
    LEFT JOIN rwiv_cover_buys c ON c.bought_at <= b.as_of
  ) ranked
  WHERE rn = 1
),

pfcfa_per_day AS (
  -- Sum of fees paid within the funding period and not yet released.
  -- Release date = 2 * 2027-12-31 - bought_at (symmetric reflection around 2027-12-31).
  SELECT
    b.day,
    COALESCE(SUM(c.net_usdc_paid), 0) AS pfcfa_usdc
  FROM base b
  LEFT JOIN rwiv_cover_buys c
    ON c.bought_at <= b.as_of
   AND CAST(c.bought_at AS date) <= DATE '2027-12-31'
   AND date_add(
         'day',
         date_diff('day', CAST(c.bought_at AS date), DATE '2027-12-31'),
         DATE '2027-12-31'
       ) >= CAST(b.day AS date)
  GROUP BY b.day
),

metrics AS (
  SELECT
    b.day,
    ub.usdc_amount,
    rb.reusde_amount,
    rb.reusde_amount * rn.reusde_nav_usde AS reusde_value_usde,  -- USDe held 1:1 with USDC; institutional par redemption assumed
    pv.pt_amount AS reusde_pt_amount,
    pv.pt_value_usde AS reusde_pt_value_usde,
    vc.value_of_cover_usdc,
    b.vault_total_balance_usdc AS total_market_cap_rwiv_usdc,
    pf.pfcfa_usdc
  FROM base b
  LEFT JOIN usdc_balance           ub  ON ub.day  = b.day
  LEFT JOIN reusde_balance         rb  ON rb.day  = b.day
  LEFT JOIN reusde_nav_per_day     rn  ON rn.day  = b.day
  LEFT JOIN pt_value_per_day       pv  ON pv.day  = b.day
  LEFT JOIN value_of_cover_per_day vc  ON vc.day  = b.day
  LEFT JOIN pfcfa_per_day          pf  ON pf.day  = b.day
)

SELECT
  day AS date,
  ROUND(
    (usdc_amount + reusde_value_usde + reusde_pt_value_usde + COALESCE(value_of_cover_usdc, 0))
    -
    (total_market_cap_rwiv_usdc + pfcfa_usdc),
    2
  ) AS nav_usdc,
  ROUND(usdc_amount + reusde_value_usde + reusde_pt_value_usde + COALESCE(value_of_cover_usdc, 0), 2) AS total_assets_usdc,
  ROUND(usdc_amount + reusde_value_usde + reusde_pt_value_usde, 2) AS total_usdc_value_in_multisig,
  ROUND(usdc_amount, 2) AS usdc_amount,
  ROUND(reusde_amount, 2) AS reusde_amount,
  ROUND(reusde_value_usde, 2) AS reusde_value_usde,
  ROUND(reusde_pt_amount, 2) AS reusde_pt_amount,
  ROUND(reusde_pt_value_usde, 2) AS reusde_pt_value_usde,
  CASE WHEN value_of_cover_usdc IS NULL THEN NULL ELSE ROUND(value_of_cover_usdc, 2) END AS value_of_cover_usdc,
  ROUND(total_market_cap_rwiv_usdc + pfcfa_usdc, 2) AS total_liabilities_usdc,
  ROUND(total_market_cap_rwiv_usdc, 2) AS total_market_cap_rwiv_usdc,
  ROUND(pfcfa_usdc, 2) AS pfcfa_usdc
FROM metrics
ORDER BY day DESC
