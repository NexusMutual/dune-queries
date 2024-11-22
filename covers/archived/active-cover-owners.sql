WITH
  cover_owners AS (
    SELECT
      1 as anchor,
      cid AS v1_cover_id,
      NULL AS v2_cover_id,
      DATE_TRUNC('day', evt_block_time) as cover_start_time,
      _userAddress as owner,
      case
        when curr = 0x45544800 then 'ETH'
        when curr = 0x44414900 then 'DAI'
      end as cover_asset,
      CAST(sumAssured AS DOUBLE) as sum_assured,
      from_unixtime(CAST(expiry AS DOUBLE)) as cover_end_time
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent as t
      INNER JOIN nexusmutual_ethereum.QuotationData_call_addCover as s ON t.evt_tx_hash = s.call_tx_hash
    WHERE
      s.call_success = TRUE
    UNION ALL
    SELECT
      1 as anchor,
      NULL AS v1_cover_id,
      CAST(output_coverId as UINT256) as v2_cover_id,
      DATE_TRUNC('day', call_block_time) AS cover_start_time,
      from_hex(
        trim(
          '"'
          FROM
            CAST(JSON_QUERY(params, 'lax $.owner') AS VARCHAR)
        )
      ) AS owner,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN 'DAI'
      END as cover_asset,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured,
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
  day_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          DATE_TRUNC('day', minute),
          symbol
      ) AS price_dollar
    FROM
      prices.usd
    WHERE
      (
        symbol = 'DAI'
        OR symbol = 'ETH'
      )
      AND minute > CAST(
        CAST('2019-05-01 00:00:00' AS TIMESTAMP) AS TIMESTAMP
      )
  ),
  eth_day_prices AS (
    SELECT
      1 as anchor,
      price_dollar AS eth_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'ETH'
    ORDER BY
      day desc
    LIMIT
      1
  ),
  dai_day_prices AS (
    SELECT
      1 as anchor,
      price_dollar AS dai_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'DAI'
    ORDER BY
      day desc
    LIMIT
      1
  ),
  owners AS (
    SELECT DISTINCT
      owner,
      SUM(
        case
          when '{{display_currency}}' = 'USD'
          AND cover_asset = 'DAI' then dai_price_dollar * sum_assured
          when '{{display_currency}}' = 'USD'
          AND cover_asset = 'ETH' then eth_price_dollar * sum_assured
          when '{{display_currency}}' = 'ETH'
          AND cover_asset = 'DAI' then dai_price_dollar * sum_assured / eth_price_dollar
          when '{{display_currency}}' = 'ETH'
          AND cover_asset = 'ETH' then sum_assured
          ELSE -1
        END
      ) OVER (
        PARTITION BY
          OWNER
      ) as total_cover
    FROM
      cover_owners as t
      INNER JOIN eth_day_prices as s ON s.anchor = t.anchor
      INNER JOIN dai_day_prices as v ON v.anchor = t.anchor
    WHERE
      cover_end_time > NOW()
  )
SELECT
  owner,
  COUNT(owner) OVER () AS unique_users,
  total_cover
FROM
  owners