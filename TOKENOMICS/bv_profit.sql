WITH
  bv_diff_swapNxmForEth AS (
    SELECT
      date_trunc('minute', a.call_block_time) as ts,
      CAST(b.output_0 AS DOUBLE) * 1E-18 AS pre_sale_nxm_supply,
      CAST(a.output_0 AS DOUBLE) * 1E-18 AS pre_sale_capital_pool_eth,
      CAST(b.output_0 - c.nxmIn AS DOUBLE) * 1E-18 AS post_sale_nxm_supply,
      CAST(a.output_0 - c.ethOut AS DOUBLE) * 1E-18 AS post_sale_capital_pool_eth,
      (
        CAST(a.output_0 - c.ethOut AS DOUBLE) / CAST(b.output_0 - c.nxmIn AS DOUBLE)
      ) - (
        CAST(a.output_0 AS DOUBLE) / CAST(b.output_0 AS DOUBLE)
      ) AS bv_diff_per_nxm,
      (
        (
          CAST(a.output_0 - c.ethOut AS DOUBLE) / CAST(b.output_0 - c.nxmIn AS DOUBLE)
        ) - (
          CAST(a.output_0 AS DOUBLE) / CAST(b.output_0 AS DOUBLE)
        )
      ) * CAST(b.output_0 - c.nxmIn AS DOUBLE) * 1E-18 AS bv_diff
    FROM
      nexusmutual_ethereum.Pool_call_getPoolValueInEth AS a
      INNER JOIN nexusmutual_ethereum.NXMToken_call_totalSupply AS b ON a.call_tx_hash = b.call_tx_hash
      INNER JOIN nexusmutual_ethereum.Ramm_evt_NxmSwappedForEth AS c ON a.call_tx_hash = c.evt_tx_hash
    WHERE
      a.call_success
      AND b.call_success
  ),
  bv_diff_swapEthForNxm AS (
    SELECT
      CAST(a.call_block_time AS TIMESTAMP) as ts,
      CAST(b.output_0 AS DOUBLE) AS pre_sale_nxm_supply,
      CAST(a.output_0 AS DOUBLE) AS pre_sale_capital_pool_eth,
      CAST(b.output_0 + c.nxmOut AS DOUBLE) AS post_sale_nxm_supply,
      CAST(a.output_0 + c.ethIn AS DOUBLE) AS post_sale_capital_pool_eth,
      (
        CAST(a.output_0 + c.ethIn AS DOUBLE) / CAST(b.output_0 + c.nxmOut AS DOUBLE)
      ) - (
        CAST(a.output_0 AS DOUBLE) / CAST(b.output_0 AS DOUBLE)
      ) AS bv_diff_per_nxm
    FROM
      nexusmutual_ethereum.Pool_call_getPoolValueInEth AS a
      INNER JOIN nexusmutual_ethereum.NXMToken_call_totalSupply AS b ON a.call_tx_hash = b.call_tx_hash
      INNER JOIN nexusmutual_ethereum.Ramm_evt_EthSwappedForNxm AS c ON a.call_tx_hash = c.evt_tx_hash
    WHERE
      a.call_success
      AND b.call_success
  ),
  prices AS (
    SELECT DISTINCT
      date_trunc('minute', minute) AS ts,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          date_trunc('minute', minute)
      ) AS price_dollar
    FROM
      prices.usd
    WHERE
      symbol = 'ETH'
      AND minute > CAST('2023-11-11' AS TIMESTAMP)
  ),
  bv_diff AS (
    SELECT
      a.ts,
      bv_diff_per_nxm,
      bv_diff,
      SUM(bv_diff) OVER (
        ORDER BY
          a.ts ASC
      ) AS cummulative_bv_diff,
      SUM(bv_diff * price_dollar) OVER (
        ORDER BY
          a.ts ASC
      ) AS cummulative_bv_diff_in_dollars
    FROM
      bv_diff_swapNxmForEth AS a
      INNER JOIN prices AS b ON a.ts = b.ts
  )
SELECT
  *
FROM
  bv_diff
WHERE
  ts >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND ts <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  ts DESC