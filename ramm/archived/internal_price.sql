WITH
  internal_price AS (
    SELECT
      date_trunc('minute', call_block_time) AS ts,
      CAST(output_internalPrice AS DOUBLE) * 1E-18 AS internal_price_nxm
    FROM
      nexusmutual_ethereum.Ramm_call_getInternalPriceAndUpdateTwap
  ),
  prices AS (
    SELECT DISTINCT
      date_trunc('minute', minute) AS ts,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          date_trunc('minute', minute)
      ) AS price_dollar
    FROM prices.usd
    WHERE symbol = 'ETH'
      AND blockchain IS NULL
      AND minute > CAST('2023-11-11' AS TIMESTAMP)
  )
SELECT
  a.ts,
  internal_price_nxm,
  price_dollar * internal_price_nxm AS internal_price_dollar,
  price_dollar 
FROM
  internal_price AS a
  INNER JOIN prices AS b ON a.ts = b.ts
WHERE
  a.ts >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND a.ts <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  a.ts DESC