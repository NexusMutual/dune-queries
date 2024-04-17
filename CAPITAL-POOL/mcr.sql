WITH
  MCR_event AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS date,
      CAST(mcrEtherx100 AS DOUBLE) * 1E-18 AS mcr_eth,
      7000 AS mcr_floor,
      0 AS mcr_cover_min
    FROM
      nexusmutual_ethereum.MCR_evt_MCREvent
  ),
  MCR_updated AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      CAST(mcr AS DOUBLE) * 1E-18 AS mcr_eth,
      CAST(mcrFloor AS DOUBLE) * 1E-18 AS mcr_floor,
      CAST(mcrETHWithGear AS DOUBLE) * 1E-18 AS mcr_cover_min
    FROM
      nexusmutual_ethereum.MCR_evt_MCRUpdated
  ),
  MCR_updated_event AS (
    SELECT
      *
    FROM
      MCR_event
    UNION
    SELECT
      *
    FROM
      MCR_updated
  ),
  day_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          symbol
      ) AS price_dollar
    FROM prices.usd
    WHERE symbol = 'ETH'
      AND coalesce(blockchain, 'ethereum') = 'ethereum'
      AND minute > CAST('2019-11-06 00:00:00' AS TIMESTAMP)
  ),
  ethereum_price_ma7 as (
    select
      day,
      price_dollar,
      avg(price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_eth
    from
      day_prices
    ORDER BY
      day DESC
  ),
  mcr AS (
    SELECT
      COALESCE(MCR_updated_event.date, ethereum_price_ma7.day) AS date,
      moving_average_eth as eth_price_dollar,
      mcr_eth,
      mcr_floor,
      mcr_cover_min
    FROM
      ethereum_price_ma7
      LEFT JOIN MCR_updated_event ON MCR_updated_event.date = ethereum_price_ma7.day
  ),
  mcr_filled_null_cnts AS (
    SELECT
      date,
      eth_price_dollar,
      mcr_eth,
      mcr_floor,
      mcr_cover_min,
      COUNT(mcr_eth) OVER (
        ORDER BY
          date
      ) AS mcr_eth_count,
      COUNT(mcr_floor) OVER (
        ORDER BY
          date
      ) AS mcr_floor_count,
      COUNT(mcr_cover_min) OVER (
        ORDER BY
          date
      ) AS mcr_cover_min_count
    FROM
      mcr
  ),
  mcr_currency AS (
    SELECT
      date,
      CASE
        WHEN '{{display_currency}}' = 'USD' THEN eth_price_dollar * FIRST_VALUE(mcr_eth) OVER (
          PARTITION BY
            mcr_eth_count
          ORDER BY
            date
        )
        WHEN '{{display_currency}}' = 'ETH' THEN FIRST_VALUE(mcr_eth) OVER (
          PARTITION BY
            mcr_eth_count
          ORDER BY
            date
        )
        ELSE -1
      END AS mcr_eth_display_curr,
      CASE
        WHEN '{{display_currency}}' = 'USD' THEN eth_price_dollar * FIRST_VALUE(mcr_floor) OVER (
          PARTITION BY
            mcr_floor_count
          ORDER BY
            date
        )
        WHEN '{{display_currency}}' = 'ETH' THEN FIRST_VALUE(mcr_floor) OVER (
          PARTITION BY
            mcr_floor_count
          ORDER BY
            date
        )
        ELSE -1
      END AS mcr_floor_display_curr,
      CASE
        WHEN '{{display_currency}}' = 'USD' THEN eth_price_dollar * FIRST_VALUE(mcr_cover_min) OVER (
          PARTITION BY
            mcr_cover_min_count
          ORDER BY
            date
        )
        WHEN '{{display_currency}}' = 'ETH' THEN FIRST_VALUE(mcr_cover_min) OVER (
          PARTITION BY
            mcr_cover_min_count
          ORDER BY
            date
        )
        ELSE -1
      END AS mcr_cover_min_display_curr
    FROM
      mcr_filled_null_cnts
  )
SELECT
  *
FROM
  mcr_currency
WHERE
  date >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND date <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  date DESC