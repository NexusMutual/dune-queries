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
      Coalesce(
        v1_product_info.product_address,
        nexusmutual."QuotationData_evt_CoverDetailsEvent"."scAdd"
      ),
      syndicate,
      product_name,
      product_type,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat(
        "QuotationData_evt_CoverDetailsEvent"."expiry",
        'second'
      ) :: interval as cover_end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
      LEFT JOIN v1_product_info ON v1_product_info.product_address = nexusmutual."QuotationData_evt_CoverDetailsEvent"."scAdd"
  ),
  cover as (
    select
      cover_end_time as date_c,
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
    WHERE
      cover_end_time > NOW()
  ),
  running_total_separated as (
    SELECT
      DISTINCT 1 as anchor,
      syndicate,
      product_name,
      product_type,
      SUM(gross_eth) OVER () as running_net_eth,
      SUM(gross_dai) OVER () as running_dai_eth,
      SUM(gross_eth) OVER (PARTITION BY product_name) as running_net_eth_product_name,
      SUM(gross_dai) OVER (PARTITION BY product_name) as running_dai_eth_product_name,
      SUM(gross_eth) OVER (PARTITION BY product_type) as running_net_eth_product_type,
      SUM(gross_dai) OVER (PARTITION BY product_type) as running_dai_eth_product_type,
      SUM(gross_eth) OVER (PARTITION BY syndicate) as running_net_eth_syndicate,
      SUM(gross_dai) OVER (PARTITION BY syndicate) as running_dai_eth_syndicate
    FROM
      cover
  ),
  average_day_ethereum_price as (
    select
      CAST(date_trunc('day', minute) as DATE) as day,
      avg(price) as eth_avg_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
    GROUP BY
      day
    ORDER BY
      day DESC
    limit
      1
  ), average_day_dai_price as (
    SELECT
      CAST(date_trunc('day', minute) as DATE) as day,
      avg(price) as dai_avg_price
    from
      prices."usd"
    where
      symbol = 'DAI'
    GROUP BY
      day
    ORDER BY
      day DESC
    limit
      1
  ), price as (
    select
      1 as anchor,
      dai_avg_price,
      eth_avg_price
    from
      average_day_ethereum_price
      INNER JOIN average_day_dai_price ON average_day_ethereum_price.day = average_day_dai_price.day
  )
SELECT
  syndicate,
  product_name,
  product_type,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * running_net_eth
    when '{{display_currency}}' = 'ETH' then running_net_eth
    ELSE -1
  END as running_net_eth_display_curr,
  case
    when '{{display_currency}}' = 'USD' then dai_avg_price * running_dai_eth
    when '{{display_currency}}' = 'ETH' then dai_avg_price * running_dai_eth / eth_avg_price
    ELSE -1
  END as running_net_dai_display_curr,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * running_net_eth_syndicate
    when '{{display_currency}}' = 'ETH' then running_net_eth_syndicate
    ELSE -1
  END as running_net_eth_syndicate_display_curr,
  case
    when '{{display_currency}}' = 'USD' then dai_avg_price * running_dai_eth_syndicate
    when '{{display_currency}}' = 'ETH' then dai_avg_price * running_dai_eth_syndicate / eth_avg_price
    ELSE -1
  END as running_net_dai_syndicate_display_curr,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * running_net_eth_product_type
    when '{{display_currency}}' = 'ETH' then running_net_eth_product_type
    ELSE -1
  END as running_net_eth_product_type_display_curr,
  case
    when '{{display_currency}}' = 'USD' then dai_avg_price * running_dai_eth_product_type
    when '{{display_currency}}' = 'ETH' then dai_avg_price * running_dai_eth_product_type / eth_avg_price
    ELSE -1
  END as running_net_dai_product_type_display_curr,
  case
    when '{{display_currency}}' = 'USD' then eth_avg_price * running_net_eth_product_name
    when '{{display_currency}}' = 'ETH' then running_net_eth_product_name
    ELSE -1
  END as running_net_eth_product_name_display_curr,
  case
    when '{{display_currency}}' = 'USD' then dai_avg_price * running_dai_eth_product_name
    when '{{display_currency}}' = 'ETH' then dai_avg_price * running_dai_eth_product_name / eth_avg_price
    ELSE -1
  END as running_net_dai_product_name_display_curr,
  case
    when '{{display_currency}}' = 'USD' then (dai_avg_price * running_dai_eth_syndicate) + (eth_avg_price * running_net_eth_syndicate)
    when '{{display_currency}}' = 'ETH' then (
      (dai_avg_price * running_dai_eth_syndicate) + (eth_avg_price * running_net_eth_syndicate)
    ) / eth_avg_price
    ELSE -1
  END as running_syndicate_display_curr,
  case
    when '{{display_currency}}' = 'USD' then (dai_avg_price * running_dai_eth_product_name) + (eth_avg_price * running_net_eth_product_name)
    when '{{display_currency}}' = 'ETH' then (
      (dai_avg_price * running_dai_eth_product_name) + (eth_avg_price * running_net_eth_product_name)
    ) / eth_avg_price
    ELSE -1
  END as running_product_name_display_curr,
  case
    when '{{display_currency}}' = 'USD' then (dai_avg_price * running_dai_eth_product_type) + (eth_avg_price * running_net_eth_product_type)
    when '{{display_currency}}' = 'ETH' then (
      (dai_avg_price * running_dai_eth_product_type) + (eth_avg_price * running_net_eth_product_type)
    ) / eth_avg_price
    ELSE -1
  END as running_product_type_display_curr,
  case
    when '{{display_currency}}' = 'USD' then (dai_avg_price * running_dai_eth) + (eth_avg_price * running_net_eth)
    when '{{display_currency}}' = 'ETH' then (
      (dai_avg_price * running_dai_eth) + (eth_avg_price * running_net_eth)
    ) / eth_avg_price
    ELSE -1
  END as running_total_display_curr
from
  price
  INNER JOIN running_total_separated ON running_total_separated.anchor = price.anchor