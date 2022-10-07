WITH
  claims as (
    SELECT
      "_claimId" as claim_id,
      "_coverId" as cover_id,
      date '1970-01-01 00:00:00' + concat("_nowtime", ' second'):: interval as claim_submit_time
    from
      nexusmutual."ClaimsData_call_addClaim"
  ),
  status as (
    SELECT
      "_claimId" as claim_id,
      "_stat" as status_no
    from
      nexusmutual."ClaimsData_call_setClaimStatus"
    WHERE
      "_stat" = 14
  ),
  cover_details as (
    select
      "cid" as cover_id,
      "evt_block_time" as start_time,
      "sumAssured" as sum_assured,
      "sumAssured" * 0.2 as assessor_rewards,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as native_cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second'):: interval as end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
  ),
  claims_status_detail as (
    SELECT
      *,
      date_trunc('day', claim_submit_time) as day
    from
      claims
      INNER JOIN status ON claims.claim_id = status.claim_id
  ),
  dollar_price_dai as (
    SELECT
      date_trunc('day', minute) as day,
      avg(price) as avg_dai_price
    from
      prices."usd"
    where
      symbol = 'DAI'
      and date_trunc('day', minute) > '2019-05-01 00:00:00'
    GROUP BY
      day
    ORDER BY
      day
  ),
  dollar_price_eth as (
    select
      date_trunc('day', minute) as day,
      avg(price) as avg_eth_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
      and date_trunc('day', minute) > '2019-05-01 00:00:00'
    GROUP BY
      day
    ORDER BY
      day
  ),
  dollar_price as (
    select
      dollar_price_dai.day as day,
      avg_eth_price,
      avg_dai_price
    from
      dollar_price_dai
      INNER JOIN dollar_price_eth ON dollar_price_dai.day = dollar_price_eth.day
  ),
  claims_status_cover_details as (
    SELECT
      *
    from
      cover_details
      INNER JOIN claims_status_detail ON claims_status_detail.cover_id = cover_details.cover_id
  )
SELECT
  *,
  case
    when native_cover_asset = 'DAI' then assessor_rewards * avg_dai_price
    when native_cover_asset = 'ETH' then assessor_rewards * avg_eth_price
  end as dollar_reward,
  case
    when native_cover_asset = 'DAI' then assessor_rewards * avg_dai_price / avg_eth_price
    when native_cover_asset = 'ETH' then assessor_rewards 
  end as eth_reward,
  case
    when native_cover_asset = 'DAI' then assessor_rewards
    when native_cover_asset = 'ETH' then assessor_rewards * avg_eth_price / avg_dai_price
  end as dai_reward
from
  claims_status_cover_details
  INNER JOIN dollar_price ON claims_status_cover_details.day = dollar_price.day