WITH
  MCR_event as (
    select
      date_trunc('day', evt_block_time) as date,
      "mcrEtherx100" * 1E-18 as mcr_eth,
      7000 as mcr_floor,
      0 as mcr_cover_min
    from
      nexusmutual."MCR_evt_MCREvent"
  ),
  MCR_updated as (
    select
      date_trunc('day', evt_block_time) as day,
      "mcr" * 1E-18 as mcr_eth,
      "mcrFloor" * 1E-18 as mcr_floor,
      "mcrETHWithGear" * 1E-18 as mcr_cover_min
    from
      nexusmutual."MCR_evt_MCRUpdated"
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
  ethereum_price_average as (
    select
      date_trunc('day', minute) as day,
      avg(price) as eth_avg_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
    GROUP BY
      day
    ORDER BY
      day
  )
SELECT
  MCR_updated_event.date as date,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * mcr_eth
    when '{{display_currency}}' = 'ETH' then mcr_eth
    ELSE -1
  END mcr_eth_display_curr,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * mcr_floor
    when '{{display_currency}}' = 'ETH' then mcr_floor
    ELSE -1
  END as mcr_floor_display_curr,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * mcr_cover_min
    when '{{display_currency}}' = 'ETH' then mcr_cover_min
    ELSE -1
  END as mcr_cover_min_display_curr
FROM
  MCR_updated_event
  INNER JOIN ethereum_price_average ON ethereum_price_average.day = MCR_updated_event.date
WHERE
  date >= '{{1. Start Date}}'
  AND date <= '{{2. End Date}}'
ORDER BY
  day DESC