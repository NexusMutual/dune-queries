WITH
  covers_details_event as (
    select
      "cid" as cover_id,
      date_trunc('minute', "evt_block_time") as start_time,
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
  cover_product_info as (
    SELECT
      cover_id,
      start_time,
      date_trunc('day', start_time) as start_time_trunc,
      date_trunc('minute', end_time) as end_time,
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
  ),
  covers as (
    select
      "_userAddress" as user_address,
      "call_tx_hash" as call_tx
    from
      nexusmutual."QuotationData_call_addCover"
    where
      call_success = true
  ),
  average_day_ethereum_price as (
    select
      date_trunc('day', minute) as day,
      avg(price) as avg_eth_price_dollar
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
      avg(price) as avg_dai_price_dollar
    from
      prices."usd"
    where
      symbol = 'DAI'
    GROUP BY
      day
    ORDER BY
      day
  )
SELECT
  cover_id,
  cover_asset,
  sum_assured,
  avg_dai_price_dollar,
  avg_eth_price_dollar,
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
    when end_time >= NOW() then 'Active'
    when end_time < NOW() then 'Expired'
  end as active,
  COUNT(user_address) OVER (
    PARTITION BY user_address
    ORDER BY
      start_time
  ) as rank_no_of_purchase,
  call_tx
from
  cover_product_info
  LEFT JOIN average_day_dai_price ON cover_product_info.start_time_trunc = average_day_dai_price.day
  LEFT JOIN average_day_ethereum_price ON cover_product_info.start_time_trunc = average_day_ethereum_price.day
  INNER JOIN covers ON cover_product_info.evt_hash = covers.call_tx
ORDER BY
  start_time DESC