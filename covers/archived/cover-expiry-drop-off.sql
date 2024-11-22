with
  cover_data as (
    SELECT
      cid AS v1_cover_id,
      NULL AS v2_cover_id,
      DATE_TRUNC('day', evt_block_time) AS cover_start_time,
      CAST(sumAssured AS DOUBLE) AS sum_assured,
      CASE
        WHEN curr = 0x45544800 THEN CAST(sumAssured AS DOUBLE)
        WHEN curr = 0x44414900 THEN CAST(0 AS DOUBLE)
      END AS gross_eth_brought,
      CASE
        WHEN curr = 0x45544800 THEN CAST(0 AS DOUBLE)
        WHEN curr = 0x44414900 THEN CAST(sumAssured AS DOUBLE)
      END AS gross_dai_brought,
      CASE
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS cover_asset,
      from_unixtime(CAST(t.expiry AS DOUBLE)) AS cover_end_time
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent as t
    UNION ALL
    SELECT
      NULL AS v1_cover_id,
      CAST(output_coverId as UINT256) as v2_cover_id,
      CAST(call_block_time AS TIMESTAMP) AS cover_start_time,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN CAST(0 AS DOUBLE)
      END as gross_eth_brought,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN CAST(0 AS DOUBLE)
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18
      END as gross_dai_brought,
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
  cover_data_expired as (
    select
      cover_asset,
      case
        when cover_asset = 'ETH' then -1 * sum_assured
        when cover_asset = 'DAI' then 0
      end as gross_eth_expired,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then -1 * sum_assured
      end as gross_dai_expired,
      date_trunc('day', cover_end_time) as date_c
    from
      cover_data as s
    where
      cover_end_time >= NOW()
  ),
  cover_data_expired_summed as (
    select
      date_c,
      SUM(gross_eth_expired) as eth_summed_expired,
      SUM(gross_dai_expired) as dai_summed_expired
    FROM
      cover_data_expired
    group by
      date_c
  ),
  starting_totals as (
    select
      1 as anchor,
      sum(gross_eth_brought) as starting_eth,
      sum(gross_dai_brought) as starting_dai
    FROM
      cover_data
    where
      cover_end_time >= NOW()
  ),
  cover_expire_to_in_future as (
    SELECT DISTINCT
      date_c,
      1 as anchor,
      SUM(gross_eth_expired) OVER (
        ORDER BY
          date_c ASC
      ) as eth_running_expiry,
      SUM(gross_dai_expired) OVER (
        ORDER BY
          date_c ASC
      ) as dai_running_expiry
    from
      cover_data_expired
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
  cover_expire_dai_eth as (
    SELECT
      1 as anchor,
      CAST(date_c as DATE) as date_c,
      eth_running_expiry,
      dai_running_expiry,
      starting_eth,
      starting_dai,
      starting_eth + eth_running_expiry as total_eth,
      starting_dai + dai_running_expiry as total_dai
    from
      cover_expire_to_in_future
      INNER JOIN starting_totals ON starting_totals.anchor = cover_expire_to_in_future.anchor
  )
select
  date_trunc('day', CAST(date_c AS TIMESTAMP)) as date_c,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * total_eth
    when '{{display_currency}}' = 'ETH' then total_eth
    ELSE -1
  END as total_eth_display_curr,
  case
    when '{{display_currency}}' = 'USD' then total_dai * dai_avg_price
    when '{{display_currency}}' = 'ETH' then total_dai * dai_avg_price / eth_avg_price
    ELSE -1
  END as total_dai_display_curr,
  case
    when '{{display_currency}}' = 'USD' then (eth_avg_price * total_eth) + (total_dai * dai_avg_price)
    when '{{display_currency}}' = 'ETH' then (
      (eth_avg_price * total_eth) + (total_dai * dai_avg_price)
    ) / eth_avg_price
    ELSE -1
  END as total_display_curr
from
  day_prices
  INNER JOIN cover_expire_dai_eth ON cover_expire_dai_eth.anchor = day_prices.anchor -- join on anchor here as we use the latest price for calclating dollar values here
ORDER by
   date_c DESC