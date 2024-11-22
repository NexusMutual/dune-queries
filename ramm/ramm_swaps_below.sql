WITH
  swaps AS (
    SELECT DISTINCT
      date_trunc('minute', evt_block_time) AS ts,
      ethOut,
      nxmIn,
      member,
      CAST(ethOut AS DOUBLE) / CAST(nxmIn AS DOUBLE) AS swap_price
    FROM
      nexusmutual_ethereum.Ramm_evt_NxmSwappedForEth
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
  ),
  swaps_below AS (
    SELECT
      a.ts,
      a.member,
      a.nxmIn * 1E-18 AS nxmIn,
      a.ethOut * 1E-18 AS ethOut,
      swap_price AS swap_price_b,
      swap_price * price_dollar AS swap_price_b_dollar
    FROM
      swaps AS a
      INNER JOIN prices AS b ON a.ts = b.ts
  )
SELECT
  *
FROM
  swaps_below
WHERE
  ts >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND ts <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  ts DESC