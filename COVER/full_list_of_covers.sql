WITH
  covers_details_event as (
    select
      "cid" as cover_id,
      "evt_block_time" as start_time,
      date '1970-01-01 00:00:00' + concat("expiry", ' second'):: interval as end_time,
      "scAdd" as user,
      "sumAssured" as sum_assured,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      "premiumNXM" as premiumNxm,
      "premium" as premium,
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
  *,
  case
    when end_time >= NOW() then True
    when end_time < NOW() then False
  end as active
from
  covers
 INNER JOIN covers_details_event ON covers.call_tx= covers_details_event.evt_hash 














  with
  add_cover as (
    select
      "premium" as prem,
      "call_block_time" as time
    from
      nexusmutual."QuotationData_call_addCover"
    where
      call_success = true
  ),
  cover_details as (
    select
      "evt_block_time" as time,
      "premium" as premium
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
  )
select
  *
from
  cover_details
  INNER JOIN add_cover ON cover_details.time = add_cover.time