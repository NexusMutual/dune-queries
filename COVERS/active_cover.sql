WITH
  cover_data AS (
    SELECT
      cid AS cover_id,
      DATE_TRUNC('day', evt_block_time) AS cover_start_time,
      CAST(sumAssured AS DOUBLE) AS sum_assured,
      CASE
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS cover_asset,
      from_unixtime(
        CAST(
          QuotationData_evt_CoverDetailsEvent.expiry AS DOUBLE
        )
      ) AS cover_end_time
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent
    UNION ALL
    SELECT
      CAST(output_coverId AS UINT256) AS cover_id,
      DATE_TRUNC('day', CAST(call_block_time AS TIMESTAMP)) AS cover_start_time,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN 'DAI'
        ELSE 'NA'
      END AS cover_asset,
      date_add(
        'second',
        CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT),
        CAST(call_block_time AS TIMESTAMP)
      ) AS cover_end_time
    FROM
      nexusmutual_ethereum.Cover_call_buyCover
    WHERE
      call_success = TRUE
  ),
  cover_data_expired AS (
    SELECT
      cover_asset,
      CASE
        WHEN cover_asset = 'ETH' THEN -1 * sum_assured
        WHEN cover_asset = 'DAI' THEN 0
      END AS gross_eth_expired,
      CASE
        WHEN cover_asset = 'ETH' THEN 0
        WHEN cover_asset = 'DAI' THEN -1 * sum_assured
      END AS gross_dai_expired,
      DATE_TRUNC('day', cover_end_time) AS date_c
    FROM
      cover_data AS s
    WHERE
      cover_end_time <= NOW()
  ),
  cover_data_buy AS (
    SELECT
      cover_asset,
      CASE
        WHEN cover_asset = 'ETH' THEN sum_assured
        WHEN cover_asset = 'DAI' THEN 0
      END AS gross_eth_brought,
      CASE
        WHEN cover_asset = 'ETH' THEN 0
        WHEN cover_asset = 'DAI' THEN sum_assured
      END AS gross_dai_brought,
      DATE_TRUNC('day', cover_start_time) AS date_c
    FROM
      cover_data
  ),
  cover_data_buy_summed AS (
    SELECT DISTINCT
      date_c,
      SUM(gross_eth_brought) AS eth_summed_buy,
      SUM(gross_dai_brought) AS dai_summed_buy
    FROM
      cover_data_buy
    GROUP BY
      date_c
  ),
  cover_data_sold_summed AS (
    SELECT DISTINCT
      date_c,
      SUM(gross_eth_expired) AS eth_summed_expired,
      SUM(gross_dai_expired) AS dai_summed_expired
    FROM
      cover_data_expired
    GROUP BY
      date_c
  ),
  cover_day_changes AS (
    SELECT
      COALESCE(
        cover_data_buy_summed.date_c,
        cover_data_sold_summed.date_c
      ) AS date_c,
      COALESCE(cover_data_buy_summed.eth_summed_buy, 0) AS eth_summed_buy,
      COALESCE(cover_data_sold_summed.eth_summed_expired, 0) AS eth_summed_expired,
      COALESCE(cover_data_buy_summed.dai_summed_buy, 0) AS dai_summed_buy,
      COALESCE(cover_data_sold_summed.dai_summed_expired, 0) AS dai_summed_expired,
      COALESCE(cover_data_buy_summed.eth_summed_buy, 0) + COALESCE(cover_data_sold_summed.eth_summed_expired, 0) AS net_eth,
      COALESCE(cover_data_buy_summed.dai_summed_buy, 0) + COALESCE(cover_data_sold_summed.dai_summed_expired, 0) AS net_dai
    FROM
      cover_data_sold_summed
      FULL JOIN cover_data_buy_summed ON cover_data_sold_summed.date_c = cover_data_buy_summed.date_c
  ),
  running_net_cover AS (
    SELECT DISTINCT
      date_c,
      net_eth,
      net_dai,
      SUM(net_eth) OVER (
        ORDER BY
          date_c
      ) AS total_eth,
      SUM(net_dai) OVER (
        ORDER BY
          date_c
      ) AS total_dai
    FROM
      cover_day_changes
  ),
  day_prices AS (
    SELECT DISTINCT
      date_trunc('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          date_trunc('day', minute),
          symbol
      ) AS price_dollar
    FROM
      prices.usd
    WHERE
      (
        symbol = 'DAI'
        OR symbol = 'ETH'
      )
      AND minute > CAST('2019-05-23' AS TIMESTAMP)
  ),
  eth_day_prices AS (
    SELECT
      day,
      price_dollar AS eth_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'ETH'
  ),
  dai_day_prices AS (
    SELECT
      day,
      price_dollar AS dai_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'DAI'
  ),
  ethereum_price_ma7 AS (
    SELECT DISTINCT
      day,
      eth_price_dollar,
      avg(eth_price_dollar) OVER (
        PARTITION BY
          day
      ) AS moving_average_eth
    FROM
      eth_day_prices
    ORDER BY
      day DESC
  ),
  dai_price_ma7 AS (
    SELECT DISTINCT
      day,
      dai_price_dollar,
      avg(dai_price_dollar) OVER (
        PARTITION BY
          day
      ) AS moving_average_dai
    FROM
      dai_day_prices
    ORDER BY
      day DESC
  ),
  price_ma AS (
    SELECT
      1 AS anchor,
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai
    FROM
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
    order by
      ethereum_price_ma7.day DESC
    LIMIT
      1
  ),
  eth_dai_totals_prices AS (
    SELECT
      date_C,
      price_ma.moving_average_eth,
      price_ma.moving_average_dai,
      total_eth,
      total_dai
    FROM
      price_ma
      INNER JOIN running_net_cover ON price_ma.anchor = 1
    ORDER BY
      day
  )
SELECT
  date_c AS day,
  moving_average_eth,
  moving_average_dai,
  CASE
    WHEN '{{display_currency}}' = 'USD' THEN moving_average_eth * total_eth
    WHEN '{{display_currency}}' = 'ETH' THEN total_eth
    ELSE -1
  END AS total_eth_display_curr,
  CASE
    WHEN '{{display_currency}}' = 'USD' THEN total_dai * moving_average_dai
    WHEN '{{display_currency}}' = 'ETH' THEN total_dai * moving_average_dai / moving_average_eth
    ELSE -1
  END AS total_dai_display_curr,
  CASE
    WHEN '{{display_currency}}' = 'USD' THEN (moving_average_eth * total_eth) + (total_dai * moving_average_dai)
    WHEN '{{display_currency}}' = 'ETH' THEN (
      (moving_average_eth * total_eth) + (total_dai * moving_average_dai)
    ) / moving_average_eth
    ELSE -1
  END AS total_display_curr
FROM
  eth_dai_totals_prices AS T
WHERE
  t.date_c >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND t.date_c <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  date_c DESC NULLS FIRST