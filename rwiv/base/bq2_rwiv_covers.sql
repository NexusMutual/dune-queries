-- RWIV Base Query 2 - Cover Buys
-- Dune query ID: 7506037
-- Every buyCover/editCover call by the VO Multisig for productId = 425 (RWIV).
-- Single source of truth for cover purchase history across Q7 and Q9.
-- Each row's fields are recorded at tx time and never rewritten by later edits
-- — this matters because query_3810247 (Active Cover Amount) retroactively
-- truncates cover_end_time / cover_period / premium_native when a later edit
-- replaces a cover. Reading BQ2 instead preserves the in-force cover's
-- original parameters.
--
-- Output columns:
--   call_block_time    - tx timestamp; the moment this cover becomes in-force
--   action             - 'new' for a fresh purchase, 'edit' for an edit of a prior cover
--   new_cover_id       - cover_id assigned to the cover purchased in this tx
--   edited_cover_id    - NULL for 'new'; the cover_id being replaced for 'edit'
--   cover_amount_usdc  - cover sum insured (raw `amount` param / 1e6)
--   period_days        - cover duration as set at buy time
--   premium_nxm        - gross full premium of the cover bought in this tx (NXM)
--   nxm_price_usdc     - protocol-internal NXM/USDC price at this tx (from same trace)
--   premium_usdc       - premium_nxm * nxm_price_usdc (gross full premium in USDC)
--   net_nxm_paid       - owner's actual NXM outflow this tx (new premium - refund of prior cover)
--   net_usdc_paid      - net_nxm_paid * nxm_price_usdc (owner's USDC-equivalent outflow)
--   call_tx_hash       - underlying tx hash
--
-- Consumers:
--   Q7 (Vault vs Cover) - latest in-force buyCover for cover_end / days_remaining /
--                         cover_amount_usdc; sums net_nxm_paid for cumulative_cover_fees_nxm.
--   Q9 (NAV)            - latest in-force buyCover for Value of Cover;
--                         sums net_usdc_paid for PFCFA.

WITH product_buys AS (
  SELECT
    c.call_tx_hash,
    c.call_block_time,
    c.output_coverId AS new_cover_id,
    CAST(JSON_EXTRACT_SCALAR(c.params, '$.coverId')           AS bigint)  AS edited_cover_id,
    from_hex(substr(JSON_EXTRACT_SCALAR(c.params, '$.owner'), 3))         AS owner_addr,
    CAST(JSON_EXTRACT_SCALAR(c.params, '$.amount')            AS double) / 1e6   AS cover_amount_usdc,
    CAST(JSON_EXTRACT_SCALAR(c.params, '$.period')            AS double) / 86400 AS period_days,
    CAST(JSON_EXTRACT_SCALAR(c.params, '$.maxPremiumInAsset') AS double) / 1e18  AS max_premium_nxm
  FROM nexusmutual_ethereum.cover_call_buycover c
  WHERE c.call_success
    AND CAST(JSON_EXTRACT_SCALAR(c.params, '$.productId') AS integer) = 425
    AND c.call_block_date >= DATE '2026-04-01'  -- first RWIV cover purchased 2026-04-24
), nxm_price AS (
  -- The exact NXM/USDC price the Cover contract used for this buyCover (no external oracle).
  SELECT call_tx_hash, output_tokenPrice / 1e6 AS nxm_price_usdc
  FROM nexusmutual_ethereum.pool_call_getinternaltokenpriceinassetandupdatetwap
  WHERE call_success
    AND assetId = 6
    AND call_block_date >= DATE '2026-04-01'
), owner_nxm_out AS (
  -- Owner's net NXM outflow in this tx (sum of Transfers from owner_addr).
  SELECT
    evt_tx_hash,
    "from" AS owner_addr,
    SUM(CAST(value AS double)) / 1e18 AS nxm_out
  FROM nexusmutual_ethereum.nxmtoken_evt_transfer
  WHERE evt_block_date >= DATE '2026-04-01'
  GROUP BY 1, 2
)
SELECT
  b.call_block_time,
  CASE WHEN b.edited_cover_id = 0 THEN 'new' ELSE 'edit' END AS action,
  b.new_cover_id,
  NULLIF(b.edited_cover_id, 0) AS edited_cover_id,
  ROUND(b.cover_amount_usdc, 2)                     AS cover_amount_usdc,
  ROUND(b.period_days, 2)                           AS period_days,
  ROUND(b.max_premium_nxm, 2)                       AS premium_nxm,
  ROUND(np.nxm_price_usdc, 4)                       AS nxm_price_usdc,
  ROUND(b.max_premium_nxm * np.nxm_price_usdc, 2)   AS premium_usdc,
  ROUND(o.nxm_out, 2)                               AS net_nxm_paid,
  ROUND(o.nxm_out * np.nxm_price_usdc, 2)           AS net_usdc_paid,
  b.call_tx_hash
FROM product_buys b
LEFT JOIN nxm_price     np ON np.call_tx_hash = b.call_tx_hash
LEFT JOIN owner_nxm_out o  ON o.evt_tx_hash   = b.call_tx_hash AND o.owner_addr = b.owner_addr
ORDER BY b.call_block_time DESC
