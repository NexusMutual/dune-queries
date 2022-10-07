select
  "coverAsset" as cover_asset,
  "coverPeriod" as cover_period,
  case
    when "coverAsset" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' then 'ETH'
    when "coverAsset" = '\x6b175474e89094c44da98b954eedeac495271d0f' then 'DAI'
  end as cover_asset,
  case
    when "coverAsset" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' then "sumAssured" * 10E-18
    when "coverAsset" = '\x6b175474e89094c44da98b954eedeac495271d0f' then "sumAssured"
  end as sum_assured,
  "evt_block_time" as cover_start_time,
  "evt_block_time" + concat("coverPeriod", ' days'):: interval as cover_end_time
from
  nexusmutual."Gateway_evt_CoverBought"






---------------------------------------------------------------------------------------------------



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
"premium" as premium
from nexusmutual."QuotationData_evt_CoverDetailsEvent"


select * from nexusmutual."QuotationData_call_addCover"

------------------------------------------------------------------------------------------------------