with
  cover_data as (
    select
      "cid" as cover_id,
      date_trunc('day', "evt_block_time") as cover_start_time,
      "sumAssured" as sum_assured,
      case
        when "curr" = '\x45544800' then "sumAssured"
        when "curr" = '\x44414900' then 0
      end as gross_eth_brought,
      case
        when "curr" = '\x45544800' then 0
        when "curr" = '\x44414900' then "sumAssured"
      end as gross_dai_brought,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as cover_end_time
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
    SELECT
      DISTINCT date_c,
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
      CAST(date_trunc('day', minute) as DATE) as day,
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
    ORDER BY
      day DESC
  ),
  price_ma as (
    select
      1 as anchor,
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai
    from
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
    ORDER BY
      day DESC
    LIMIT
      1
  ), cover_expire_dai_eth as (
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
from
  price_ma
  INNER JOIN cover_expire_dai_eth ON cover_expire_dai_eth.anchor = price_ma.anchor -- join on anchor here as we use the latest price for calclating dollar values here
ORDER by
  day DESC