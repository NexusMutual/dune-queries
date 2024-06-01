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
  cover_product_info as (
    SELECT
      cover_id,
      start_time,
      end_time,
      sum_assured,
      cover_asset,
      premiumNxm,
      premium,
      evt_hash,
      v1_product_info.product_address,
      syndicate,
      product_name,
      product_type
    from
      covers_details_event
      INNER JOIN v1_product_info ON v1_product_info.product_address = covers_details_event.productContract
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
  DISTINCT user_address,
  COUNT(user_address) OVER (PARTITION BY user_address) as rank_no_of_purchase,
  SUM(premiumNxm) OVER (PARTITION BY user_address) as premium_per_user
from
  cover_product_info
  INNER JOIN covers ON covers.call_tx = cover_product_info.evt_hash