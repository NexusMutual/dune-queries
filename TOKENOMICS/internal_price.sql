WITH
  internal_price AS (
    SELECT
      date_trunc('minute', call_block_time) AS ts,
      CAST(output_internalPrice AS DOUBLE) AS internal_price_eth
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
  internal_price_eth,
  internal_price_eth * price_dollar AS internal_price_dollar
FROM
  internal_price AS a
  INNER JOIN prices AS b ON a.ts = b.ts