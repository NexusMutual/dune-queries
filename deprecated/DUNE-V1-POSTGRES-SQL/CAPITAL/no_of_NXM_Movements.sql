WITH
  claims as (
    SELECT
      "_claimId" as claim_id,
      "_coverId" as coverId,
      date '1970-01-01 00:00:00' + concat("_nowtime", ' second'):: interval as claim_submit_time
    from
      nexusmutual."ClaimsData_call_addClaim"
  ),
  cover as (
    select
      "cid" as cover_id,
      "evt_block_time" as start_time,
      "sumAssured" as sum_assured,
      "sumAssured" * 0.2 as assessor_rewards,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second'):: interval as end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
  )
select
  *
from
  nexusmutual."ClaimsReward_call_closeClaim"