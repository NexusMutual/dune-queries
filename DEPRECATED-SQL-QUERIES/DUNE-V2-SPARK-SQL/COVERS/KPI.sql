WITH
  day_prices AS (
    SELECT
      DISTINCT date_trunc('day', minute) AS day,
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
      AND minute > '2019-01-01 00:00:00':: TIMESTAMP
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
  cover AS (
    SELECT
      cid AS cover_id,
      date_trunc('day', evt_block_time) as cover_start_time,
      premium * 1E-18 as premium,
      date_part(
        'day',
        date '1970-01-01 00:00:00' + concat(expiry, ' second'):: interval - date_trunc('day', evt_block_time)
      ) as cover_period,
      date '1970-01-01 00:00:00' + concat(expiry, ' second'):: interval as cover_end_time
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent
  )
SELECT
  DISTINCT -- premium * 365 days / cover in days-- premium * 365 days / cover in days
  case
    when '{{display_currency}}' = 'USD' then SUM(
      premium * eth_price_dollar * 365 * 60 * 60 * 24 / cover_period
    ) OVER ()
    when '{{display_currency}}' = 'ETH' then SUM(premium * 365 * 60 * 60 * 24 / cover_period) OVER () -- premium * 365 days / cover in days-- premium * 365 days / cover in days
    ELSE -1
  END as premium
FROM
  cover
  INNER JOIN dai_day_prices ON dai_day_prices.day = cover.cover_start_time
  INNER JOIN eth_day_prices ON eth_day_prices.day = cover.cover_start_time
WHERE
  cover_end_time > NOW()