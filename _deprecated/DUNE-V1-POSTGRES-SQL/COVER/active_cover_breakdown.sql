with
  v1_product_info as (
    select
      "contract_address" as product_address,
      "syndicate" as syndicate,
      "name" as product_name,
      "type" as product_type
    from
      dune_user_generated.nexus_v1_product_info_view
  ),
  cover_data as (
    select
      "cid" as cover_id,
      "evt_block_time" as cover_start_time,
      "sumAssured" as sum_assured,
      "scAdd" as productContract,
      syndicate,
      product_name,
      product_type,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as cover_end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
      INNER JOIN v1_product_info ON v1_product_info.product_address = nexusmutual."QuotationData_evt_CoverDetailsEvent"."scAdd"
  ),
  cover_data_expired as (
    select
      cover_end_time as date_c,
      cover_id,
      syndicate,
      product_name,
      product_type,
      cover_asset,
      case
        when cover_asset = 'ETH' then -1 * sum_assured
        when cover_asset = 'DAI' then 0
      end as gross_eth,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then -1 * sum_assured
      end as gross_dai
    from
      cover_data as s
    WHERE
      date_trunc('day', cover_end_time) < NOW()
  ),
  cover_data_buy as (
    select
      cover_start_time as date_c,
      cover_id,
      syndicate,
      product_name,
      product_type,
      cover_asset,
      case
        when cover_asset = 'ETH' then sum_assured
        when cover_asset = 'DAI' then 0
      end as gross_eth,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then sum_assured
      end as gross_dai
    from
      cover_data
  ),
  cover as (
    SELECT
      *
    from
      cover_data_expired
    UNION
    SELECT
      *
    from
      cover_data_buy
  ),
  running_total_separated as (
    SELECT
      date_c,
      cover_id,
      syndicate,
      product_name,
      product_type,
      SUM(gross_eth) OVER (
        ORDER BY
          date_c
      ) as running_net_eth,
      SUM(gross_dai) OVER (
        ORDER BY
          date_c
      ) as running_dai_eth,
      SUM(gross_eth) OVER (
        PARTITION BY product_name
        ORDER BY
          date_c
      ) as running_net_eth_product_name,
      SUM(gross_dai) OVER (
        PARTITION BY product_name
        ORDER BY
          date_c
      ) as running_dai_eth_product_name,
      SUM(gross_eth) OVER (
        PARTITION BY product_type
        ORDER BY
          date_c
      ) as running_net_eth_product_type,
      SUM(gross_dai) OVER (
        PARTITION BY product_type
        ORDER BY
          date_c
      ) as running_dai_eth_product_type,
      SUM(gross_eth) OVER (
        PARTITION BY syndicate
        ORDER BY
          date_c
      ) as running_net_eth_syndicate,
      SUM(gross_dai) OVER (
        PARTITION BY product_type
        ORDER BY
          date_c
      ) as running_dai_eth_syndicate
    FROM
      cover
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
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai
    from
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
  )
SELECT
  date_c,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_eth * running_net_eth
    when '{{display_currency}}' = 'ETH' then running_net_eth
    ELSE -1
  END as running_net_eth_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_dai * running_dai_eth
    when '{{display_currency}}' = 'ETH' then moving_average_dai * running_dai_eth / moving_average_eth
    ELSE -1
  END as running_net_dai_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_eth * running_net_eth_syndicate
    when '{{display_currency}}' = 'ETH' then running_net_eth_syndicate
    ELSE -1
  END as running_net_eth_syndicate_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_dai * running_dai_eth_syndicate
    when '{{display_currency}}' = 'ETH' then moving_average_dai * running_dai_eth_syndicate / moving_average_eth
    ELSE -1
  END as running_net_dai_syndicate_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_eth * running_net_eth_product_type
    when '{{display_currency}}' = 'ETH' then running_net_eth_product_type
    ELSE -1
  END as running_net_eth_product_type_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_dai * running_dai_eth_product_type
    when '{{display_currency}}' = 'ETH' then moving_average_dai * running_dai_eth_product_type / moving_average_eth
    ELSE -1
  END as running_net_dai_product_type_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_eth * running_net_eth_product_name
    when '{{display_currency}}' = 'ETH' then running_net_eth_product_name
    ELSE -1
  END as running_net_eth_product_name_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then moving_average_dai * running_dai_eth_product_name
    when '{{display_currency}}' = 'ETH' then moving_average_dai * running_dai_eth_product_name / moving_average_eth
    ELSE -1
  END as running_net_dai_product_name_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then (moving_average_dai * running_dai_eth_syndicate) + (moving_average_eth * running_net_eth_syndicate)
    when '{{display_currency}}' = 'ETH' then (
      (moving_average_dai * running_dai_eth_syndicate) + (moving_average_eth * running_net_eth_syndicate)
    ) / moving_average_eth
    ELSE -1
  END as running_syndicate_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then (moving_average_dai * running_dai_eth_product_name) + (moving_average_eth * running_net_eth_product_name)
    when '{{display_currency}}' = 'ETH' then (
      (moving_average_dai * running_dai_eth_product_name) + (moving_average_eth * running_net_eth_product_name)
    ) / moving_average_eth
    ELSE -1
  END as running_product_name_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then (moving_average_dai * running_dai_eth_product_type) + (moving_average_eth * running_net_eth_product_type)
    when '{{display_currency}}' = 'ETH' then (
      (moving_average_dai * running_dai_eth_product_type) + (moving_average_eth * running_net_eth_product_type)
    ) / moving_average_eth
    ELSE -1
  END as running_product_type_display_curr,
  case
    when '{{display_currency}}' = 'DOLLAR' then (moving_average_dai * running_dai_eth) + (moving_average_eth * running_net_eth)
    when '{{display_currency}}' = 'ETH' then (
      (moving_average_dai * running_dai_eth) + (moving_average_eth * running_net_eth)
    ) / moving_average_eth
    ELSE -1
  END as running_total_display_curr
from
  price_ma
  INNER JOIN running_total_separated ON date_trunc('day', running_total_separated.date_c) = price_ma.day
WHERE
  price_ma.day >= '{{1. Start Date}}'
  AND price_ma.day <= '{{2. End Date}}'
ORDER by
  day DESC