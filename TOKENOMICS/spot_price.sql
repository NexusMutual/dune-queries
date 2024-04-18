WITH
  spot_price AS (
    SELECT
      date_trunc('minute', call_block_time) AS ts,
      CAST(output__ethReserve AS DOUBLE) / CAST(output_nxmA AS DOUBLE) AS spot_price_eth_a,
      CAST(output__ethReserve AS DOUBLE) / CAST(output_nxmB AS DOUBLE) AS spot_price_eth_b
    FROM
      nexusmutual_ethereum.Ramm_call_getReserves
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
      AND coalesce(blockchain, 'ethereum') = 'ethereum'
      AND minute > CAST('2023-11-11' AS TIMESTAMP)
  )
SELECT
  a.ts,
  spot_price_eth_a,
  spot_price_eth_b,
  spot_price_eth_a * price_dollar AS spot_price_dollar_a,
  spot_price_eth_b * price_dollar AS spot_price_dollar_b
FROM
  spot_price AS a
  INNER JOIN prices AS b ON a.ts = b.ts