with
  cover_data as (
    select
      cid as cover_id,
      date_trunc('day', evt_block_time) as cover_start_time,
      sumAssured as sum_assured,
      expiry,
      case
        when curr = '0x45544800' then 'ETH'
        when curr = '0x44414900' then 'DAI'
      end as cover_asset,
      from_unixtime(expiry) as cover_end_time
    from
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent
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
      cover_data
    WHERE
      date_trunc('day', cover_end_time) <= NOW()
  ),
  cover_data_buy as (
    select
      cover_asset,
      case
        when cover_asset = 'ETH' then sum_assured
        when cover_asset = 'DAI' then 0
      end as gross_eth_brought,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then sum_assured
      end as gross_dai_brought,
      date_trunc('day', cover_start_time) as date_c
    from
      cover_data
  ),
  cover_data_buy_summed as (
    select
      date_c,
      SUM(gross_eth_brought) as eth_summed_buy,
      SUM(gross_dai_brought) as dai_summed_buy
    FROM
      cover_data_buy
    group by
      date_c
  ),
  cover_data_sold_summed as (
    select
      date_c,
      SUM(gross_eth_expired) as eth_summed_expired,
      SUM(gross_dai_expired) as dai_summed_expired
    FROM
      cover_data_expired
    group by
      date_c
  ),
  cover_day_changes as (
    SELECT
      COALESCE(
        cover_data_buy_summed.date_c,
        cover_data_sold_summed.date_c
      ) as date_c,
      COALESCE(cover_data_buy_summed.eth_summed_buy, 0) as eth_summed_buy,
      COALESCE(cover_data_sold_summed.eth_summed_expired, 0) as eth_summed_expired,
      COALESCE(cover_data_buy_summed.dai_summed_buy, 0) as dai_summed_buy,
      COALESCE(cover_data_sold_summed.dai_summed_expired, 0) as dai_summed_expired,
      COALESCE(cover_data_buy_summed.eth_summed_buy, 0) + COALESCE(cover_data_sold_summed.eth_summed_expired, 0) as net_eth,
      COALESCE(cover_data_buy_summed.dai_summed_buy, 0) + COALESCE(cover_data_sold_summed.dai_summed_expired, 0) as net_dai
    from
      cover_data_sold_summed
      FULL JOIN cover_data_buy_summed ON cover_data_sold_summed.date_c = cover_data_buy_summed.date_c
  ),
  running_net_cover as (
    select
      date_c,
      net_eth,
      net_dai,
      sum(net_eth) over (
        order by
          date_c
      ) as total_eth,
      sum(net_dai) over (
        order by
          date_c
      ) as total_dai
    FROM
      cover_day_changes
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
  ),
  ethereum_price_ma7 as (
    select
      day,
      eth_price_dollar,
      avg(eth_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_eth
    from
      eth_day_prices
    ORDER BY
      day DESC
  ),
  dai_price_ma7 as (
    select
      day,
      dai_price_dollar,
      avg(dai_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_dai
    from
      dai_day_prices
    ORDER BY
      day DESC
  ),
  price_ma as (
    select
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai
    from
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
  ),
  eth_dai_totals_prices as (
    select
      date_c,
      price_ma.moving_average_eth,
      price_ma.moving_average_dai,
      total_eth,
      total_dai
    from
      price_ma
      INNER JOIN running_net_cover ON price_ma.day = running_net_cover.date_C
    ORDER by
      day
  )
SELECT
  date_c as day,
  case
    when '{{display_currency}}' = 'USD' then moving_average_eth * total_eth
    when '{{display_currency}}' = 'ETH' then total_eth
    ELSE -1
  END as total_eth_display_curr,
  case
    when '{{display_currency}}' = 'USD' then total_dai * moving_average_dai
    when '{{display_currency}}' = 'ETH' then total_dai * moving_average_dai / moving_average_eth
    ELSE -1
  END as total_dai_display_curr,
  case
    when '{{display_currency}}' = 'USD' then (moving_average_eth * total_eth) + (total_dai * moving_average_dai)
    when '{{display_currency}}' = 'ETH' then (
      (moving_average_eth * total_eth) + (total_dai * moving_average_dai)
    ) / moving_average_eth
    ELSE -1
  END as total_display_curr
FROM
  eth_dai_totals_prices AS t
WHERE
  t.date_c >= '{{Start Date}}'
  AND t.date_c <= '{{End Date}}'
ORDER BY
  date_c DESC