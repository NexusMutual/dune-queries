WITH
  dai_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          symbol
      ) AS dai_price_dollar
    FROM
      prices.usd
    WHERE
      (symbol = 'DAI')
      AND minute > CAST(
        CAST('2019-05-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP
      )
  ),
  eth_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          symbol
      ) AS eth_price_dollar
    FROM
      prices.usd
    WHERE
      (symbol = 'ETH')
      AND minute > CAST(
        CAST('2019-05-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP
      )
  ),
  covers_details_event AS (
    SELECT
      DATE_TRUNC('day', CAST(evt_block_time AS TIMESTAMP)) AS cover_start_time,
      CASE
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS cover_asset,
      CAST(sumAssured AS DOUBLE) AS sum_assured
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent
    UNION ALL
    SELECT
      DATE_TRUNC('day', CAST(call_block_time AS TIMESTAMP)) AS cover_start_time,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN 'DAI'
        ELSE 'NA'
      END AS cover_asset,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured
    FROM
      nexusmutual_ethereum.Cover_call_buyCover AS t
    WHERE
      t.call_success = TRUE
  ),
  total_cover_underwritten AS (
    SELECT
      t.cover_start_time,
      eth_price_dollar,
      dai_price_dollar,
      CASE
        WHEN cover_asset = 'ETH' THEN sum_assured
        ELSE 0
      END AS eth_cover_underwritten,
      CASE
        WHEN cover_asset = 'DAI' THEN sum_assured
        ELSE 0
      END AS dai_cover_underwritten
    FROM
      covers_details_event AS t
      INNER JOIN eth_prices AS e ON e.day = t.cover_start_time
      INNER JOIN dai_prices AS d ON d.day = t.cover_start_time
  )
SELECT DISTINCT
  SUM(
    CASE
      WHEN '{{display_currency}}' = 'USD' THEN eth_cover_underwritten * eth_price_dollar
      WHEN '{{display_currency}}' = 'ETH' THEN eth_cover_underwritten
      ELSE -1
    END
  ) OVER () AS total_eth_cover_amount,
  SUM(
    CASE
      WHEN '{{display_currency}}' = 'USD' THEN dai_cover_underwritten * dai_price_dollar
      WHEN '{{display_currency}}' = 'ETH' THEN dai_cover_underwritten * dai_price_dollar / eth_price_dollar
      ELSE -1
    END
  ) OVER () AS total_dai_cover_amount,
  SUM(
    CASE
      WHEN '{{display_currency}}' = 'USD' THEN (eth_cover_underwritten * eth_price_dollar) + (dai_cover_underwritten * dai_price_dollar)
      WHEN '{{display_currency}}' = 'ETH' THEN eth_cover_underwritten + dai_cover_underwritten * dai_price_dollar / eth_price_dollar
      ELSE -1
    END
  ) OVER () AS total_cover_amount
FROM
  total_cover_underwritten AS t