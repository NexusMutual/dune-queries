WITH
  cover_data as (
    SELECT
      1 AS anchor,
      cid AS v1_cover_id,
      NULL AS v2_cover_id,
      DATE_TRUNC('day', evt_block_time) AS cover_start_time,
      CAST(sumAssured AS DOUBLE) AS sum_assured,
      CASE
        WHEN curr = 0x45544800 THEN CAST(sumAssured AS DOUBLE)
        WHEN curr = 0x44414900 THEN CAST(0 AS DOUBLE)
      END AS gross_eth,
      CASE
        WHEN curr = 0x45544800 THEN CAST(0 AS DOUBLE)
        WHEN curr = 0x44414900 THEN CAST(sumAssured AS DOUBLE)
      END AS gross_dai,
      CASE
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS cover_asset,
      from_unixtime(CAST(t.expiry AS DOUBLE)) AS cover_end_time
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent as t
    UNION ALL
    SELECT
      1 AS anchor,
      NULL AS v1_cover_id,
      CAST(output_coverId as UINT256) as v2_cover_id,
      CAST(call_block_time AS TIMESTAMP) AS cover_start_time,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN CAST(0 AS DOUBLE)
      END as gross_eth,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN CAST(0 AS DOUBLE)
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18
      END as gross_dai,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN 'DAI'
      END as cover_asset,
      date_add(
        'second',
        CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT),
        CAST(call_block_time AS TIMESTAMP)
      ) AS cover_end_time
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as t
    WHERE
      t.call_success = TRUE
  ),
  dai_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          symbol
      ) as price_dollar
    FROM
      prices.usd
    WHERE
      (symbol = 'DAI')
      AND minute > CAST(
        CAST('2019-05-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP
      )
    ORDER BY
      DATE_TRUNC('day', minute) DESC
    LIMIT
      1
  ),
  eth_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          symbol
      ) as price_dollar
    FROM
      prices.usd
    WHERE
      (symbol = 'ETH')
      AND minute > CAST(
        CAST('2019-05-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP
      )
    ORDER BY
      DATE_TRUNC('day', minute) DESC
    LIMIT
      1
  ),
  day_prices AS (
    SELECT
      1 AS anchor,
      t.price_dollar AS eth_avg_price,
      s.price_dollar AS dai_avg_price
    FROM
      eth_prices AS t
      INNER JOIN dai_prices AS s ON t.day = s.day
  ),
  weigthed_cal as (
    SELECT
      (eth_avg_price * gross_eth) + (gross_dai * dai_avg_price) as dollar_total,
      date_diff('day', NOW(), cover_end_time) as diff,
      cover_end_time,
      NOW(),
      date_diff('day', NOW(), cover_end_time) * (
        (eth_avg_price * gross_eth) + (gross_dai * dai_avg_price)
      ) as day_weighted_dollar
    FROM
      cover_data as t
      JOIN day_prices as s ON t.anchor = s.anchor
    WHERE
      cover_end_time > NOW()
  )
select
  *,
  SUM(day_weighted_dollar) OVER () / SUM(dollar_total) OVER () as total_dollar_weighted
FROM
  weigthed_cal