with
  cover_data as (
    select
      "cid" as cover_id,
      "evt_block_time" as cover_start_time,
      "sumAssured" as sum_assured,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second'):: interval as cover_end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
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
    WHERE
      date_trunc('day', cover_end_time) < NOW()
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
      cover_data_buy_summed.date_c,
      cover_data_buy_summed.eth_summed_buy,
      cover_data_sold_summed.eth_summed_expired,
      cover_data_buy_summed.dai_summed_buy,
      cover_data_sold_summed.dai_summed_expired,
      cover_data_buy_summed.eth_summed_buy + cover_data_sold_summed.eth_summed_expired as net_eth,
      cover_data_buy_summed.dai_summed_buy + cover_data_sold_summed.dai_summed_expired as net_dai
    from
      cover_data_sold_summed
      JOIN cover_data_buy_summed ON cover_data_sold_summed.date_c = cover_data_buy_summed.date_c
  ),
  running_net_cover as (
    select
      date_c,
      net_eth,
      net_dai,
      sum(net_eth) over (
        order by
          date_c asc rows between unbounded preceding
          and current row
      ) as total_eth,
      sum(net_dai) over (
        order by
          date_c asc rows between unbounded preceding
          and current row
      ) as total_dai
    FROM
      cover_day_changes
  ),
  average_day_ethereum_price as (
    select
      CAST(date_trunc('day', minute) as DATE) as day,
      avg(price) as avg_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
    GROUP BY
      day
    ORDER BY
      day
  ),
  average_day_dai_price as (
    SELECT
      date_trunc('day', minute) as day,
      avg(price) as avg_price
    from
      prices."usd"
    where
      symbol = 'DAI'
    GROUP BY
      day
    ORDER BY
      day
  ),
  ethereum_price_ma7 as (
    select
      day,
      avg_price,
      avg(avg_price) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_eth
    from
      average_day_ethereum_price
    WHERE
      day > '2019-05-01 00:00:00'
    ORDER BY
      day DESC
  ),
  dai_price_ma7 as (
    select
      day,
      avg_price,
      avg(avg_price) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_dai
    from
      average_day_dai_price
    WHERE
      day > '2019-05-01 00:00:00'
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
      day,
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
  day,
  moving_average_eth * total_eth as total_eth_dollars,
  moving_average_dai * total_dai as total_dai_dollars,
  moving_average_eth * total_eth + moving_average_dai * total_dai as total_dollars
FROM
  eth_dai_totals_prices
ORDER by
  day