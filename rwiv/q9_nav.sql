-- RWIV Q9 - NAV (daily)
-- Dune query ID: 7396288
-- Daily Vault Net Asset Value snapshot.
--   Asset side:     USDC + reUSDe balances at the VO multisig + un-amortised cover value.
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
--   - Cover record: query_3810247 (filtered to product_name = 'Real World Insurance Vault').
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

rwiv_covers AS (
  SELECT
    cover_id,
    cover_start_time,
    cover_end_time,
    premium_native,
    cover_period
  FROM query_3810247
  WHERE product_name = 'Real World Insurance Vault'
),

active_cover_per_day_ranked AS (
  SELECT
    b.day,
    b.as_of,
    rc.cover_end_time,
    rc.premium_native,
    rc.cover_period,
    ROW_NUMBER() OVER (PARTITION BY b.day ORDER BY rc.cover_start_time DESC) AS rn
  FROM base b
  LEFT JOIN rwiv_covers rc
    ON rc.cover_start_time <= b.as_of
   AND rc.cover_end_time   >  b.as_of
),

active_cover_per_day AS (
  SELECT day, as_of, cover_end_time, premium_native, cover_period
  FROM active_cover_per_day_ranked
  WHERE rn = 1
),

pfcfa_per_day AS (
  SELECT
    b.day,
    COALESCE(SUM(rc.premium_native), 0) AS pfcfa_usdc
  FROM base b
  LEFT JOIN rwiv_covers rc
    ON rc.cover_start_time <= b.as_of
   AND CAST(rc.cover_start_time AS date) <= DATE '2027-12-31'
   AND date_add(
         'day',
         date_diff('day', CAST(rc.cover_start_time AS date), DATE '2027-12-31'),
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
    CASE
      WHEN ac.cover_end_time IS NULL THEN NULL
      ELSE ac.premium_native * date_diff('day', b.as_of, ac.cover_end_time) / ac.cover_period
    END AS value_of_cover_usdc,
    b.vault_total_balance_usdc AS total_market_cap_rwiv_usdc,
    pf.pfcfa_usdc
  FROM base b
  LEFT JOIN usdc_balance         ub  ON ub.day  = b.day
  LEFT JOIN reusde_balance       rb  ON rb.day  = b.day
  LEFT JOIN reusde_nav_per_day   rn  ON rn.day  = b.day
  LEFT JOIN active_cover_per_day ac  ON ac.day  = b.day
  LEFT JOIN pfcfa_per_day        pf  ON pf.day  = b.day
)

SELECT
  day AS date,
  ROUND(
    (usdc_amount + reusde_value_usde + COALESCE(value_of_cover_usdc, 0))
    -
    (total_market_cap_rwiv_usdc + pfcfa_usdc),
    2
  ) AS nav_usdc,
  ROUND(usdc_amount + reusde_value_usde + COALESCE(value_of_cover_usdc, 0), 2) AS total_assets_usdc,
  ROUND(usdc_amount + reusde_value_usde, 2) AS total_usdc_value_in_multisig,
  ROUND(usdc_amount, 2) AS usdc_amount,
  ROUND(reusde_amount, 2) AS reusde_amount,
  ROUND(reusde_value_usde, 2) AS reusde_value_usde,
  CASE WHEN value_of_cover_usdc IS NULL THEN NULL ELSE ROUND(value_of_cover_usdc, 2) END AS value_of_cover_usdc,
  ROUND(total_market_cap_rwiv_usdc + pfcfa_usdc, 2) AS total_liabilities_usdc,
  ROUND(total_market_cap_rwiv_usdc, 2) AS total_market_cap_rwiv_usdc,
  ROUND(pfcfa_usdc, 2) AS pfcfa_usdc
FROM metrics
ORDER BY day DESC
