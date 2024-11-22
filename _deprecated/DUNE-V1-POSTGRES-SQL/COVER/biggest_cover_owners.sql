WITH
  covers_details_event as (
    select
      "cid" as cover_id,
      "evt_block_time" as start_time,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as end_time,
      "scAdd" as productContract,
      "sumAssured" as sum_assured,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      "premiumNXM" * 1E-18 as premiumNxm,
      "premium" * 1E-18 as premium,
      "evt_tx_hash" as evt_hash
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
  ),
  v1_product_info as (
    select
      "contract_address" as product_address,
      "syndicate" as syndicate,
      "name" as product_name,
      "type" as product_type
    from
      dune_user_generated.nexus_v1_product_info_view
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
  ),
  cover_product_info as (
    SELECT
      cover_id,
      date_trunc('day', start_time) as start_time,
      end_time,
      sum_assured,
      cover_asset,
      premiumNxm,
      premium,
      evt_hash,
      Coalesce(
        v1_product_info.product_address,
        covers_details_event.productContract
      ) as product_address,
      syndicate,
      product_name,
      product_type
    from
      covers_details_event
      FULL JOIN v1_product_info ON v1_product_info.product_address = covers_details_event.productContract
    where
      end_time > now()
  ),
  cover_product_amount as (
    SELECT
      *,
      case
        when cover_asset = 'ETH' then sum_assured * price_ma.moving_average_eth
        when cover_asset = 'DAI' then sum_assured * price_ma.moving_average_dai
      end as net_dollar
    FROM
      cover_product_info
      INNER JOIN price_ma ON price_ma.day = cover_product_info.start_time
  ),
  covers as (
    select
      "_userAddress" as user_address,
      "call_tx_hash" as call_tx
    from
      nexusmutual."QuotationData_call_addCover"
    where
      call_success = true
  )
SELECT
  *,
  cover_id,
  cover_asset,
  sum_assured,
  premiumNxm,
  premium,
  syndicate,
  product_type,
  product_name,
  start_time,
  TO_CHAR(start_time, 'mon') as start_month,
  end_time,
  user_address,
  end_time - start_time as period,
  case
    when end_time >= NOW() then True
    when end_time < NOW() then False
  end as active,
  COUNT(user_address) OVER (
    PARTITION BY user_address
    ORDER BY
      start_time
  ) as rank_no_of_purchase,
  call_tx
from
  cover_product_amount
  INNER JOIN covers ON covers.call_tx = cover_product_amount.evt_hash
ORDER BY
  start_time DESC