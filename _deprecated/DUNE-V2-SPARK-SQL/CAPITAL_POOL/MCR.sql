WITH
  MCR_event as (
    select
      date_trunc('day', evt_block_time) as date,
      mcrEtherx100 * 1E-18 as mcr_eth,
      7000 as mcr_floor,
      0 as mcr_cover_min
    from
      nexusmutual_ethereum.MCR_evt_MCREvent
  ),
  MCR_updated as (
    select
      date_trunc('day', evt_block_time) as day,
      mcr * 1E-18 as mcr_eth,
      mcrFloor * 1E-18 as mcr_floor,
      mcrETHWithGear * 1E-18 as mcr_cover_min
    from
        nexusmutual_ethereum.MCR_evt_MCRUpdated
  ),
  MCR_updated_event as (
    select
      *
    from
      MCR_event
    UNION
    select
      *
    from
      MCR_updated
  ),
    day_prices as (
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
      price_dollar as eth_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'ETH'
  ),
  dai_day_prices AS (
    SELECT
      day,
      price_dollar as dai_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'DAI'
  )
SELECT
  MCR_updated_event.date as date,
  case
    when '{{display_currency}}' = 'USD' then eth_price_dollar * mcr_eth
    when '{{display_currency}}' = 'ETH' then mcr_eth
    ELSE -1
  END mcr_eth_display_curr,
  case
    when '{{display_currency}}' = 'USD' then eth_price_dollar * mcr_floor
    when '{{display_currency}}' = 'ETH' then mcr_floor
    ELSE -1
  END as mcr_floor_display_curr,
  case
    when '{{display_currency}}' = 'USD' then eth_price_dollar * mcr_cover_min
    when '{{display_currency}}' = 'ETH' then mcr_cover_min
    ELSE -1
  END as mcr_cover_min_display_curr
FROM
  MCR_updated_event
  INNER JOIN eth_day_prices ON eth_day_prices.day = MCR_updated_event.date
  INNER JOIN dai_day_prices ON dai_day_prices.day = MCR_updated_event.date
WHERE
  date >= '{{1. Start Date}}'
  AND date <= '{{2. End Date}}'
ORDER BY
  date DESC