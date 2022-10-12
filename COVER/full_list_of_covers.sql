WITH
  covers_details_event as (
    select
      "cid" as cover_id,
      "evt_block_time" as start_time,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as end_time,
      "scAdd" as user,
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
  cover_id,
  cover_asset,
  sum_assured,
  premiumNxm,
  premium,
  'v1' as syndicate,
  'v1' as product_class,
  'v1' as product,
  start_time,
  1 as start_month,
  end_time,
  user_address,
  end_time - start_time as period,
  case
    when end_time >= NOW() then True
    when end_time < NOW() then False
  end as active,
  1 as rank_no,
  call_tx
from
  covers
  INNER JOIN covers_details_event ON covers.call_tx = covers_details_event.evt_hash