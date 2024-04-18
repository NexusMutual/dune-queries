WITH
  book_value_eth AS (
    SELECT
      CAST(call_block_time AS TIMESTAMP) AS ts,
      CAST(output_bookValue AS DOUBLE) AS book_value_eth
    FROM
      nexusmutual_ethereum.Ramm_call_getBookValue AS a
    WHERE
      call_success
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
  book_value_eth,
  book_value_eth * price_dollar AS book_value_dollar
FROM
  book_value_eth AS a
  INNER JOIN prices AS b ON a.ts = b.ts