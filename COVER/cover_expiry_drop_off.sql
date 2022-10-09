with
  cover_data as (
    select
      "cid" as cover_id,
      "evt_block_time" as cover_start_time,
      "sumAssured" as sum_assured,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as cover_end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
  ),
  cover_data_expired as (
    select
      cover_asset,
      case
        when cover_asset = 'ETH' then -1 * sum_assured
        when cover_asset = 'DAI' then 0
        ELSE 0
      end as gross_eth,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then -1 * sum_assured
        ELSE 0
      end as gross_dai,
      date_trunc('day', cover_end_time) as date_c
    from
      cover_data as s
  ),
  cover_data_buy as (
    select
      date_trunc('day', cover_start_time) as date_c,
      cover_asset,
      case
        when cover_asset = 'ETH' then sum_assured
        when cover_asset = 'DAI' then 0
        ELSE 0
      end as gross_eth,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then sum_assured
        ELSE 0
      end as gross_dai
    from
      cover_data
  ),
  cover_data_summary as (
    select
      date_c,
      gross_eth,
      gross_dai
    from
      cover_data_expired
    where
      date_c <= NOW()
    UNION
    select
      date_c,
      gross_eth,
      gross_dai
    from
      cover_data_buy
  ),
  cover_expire_to_in_future as (
    SELECT
      1 as anchor,
      date_c,
      gross_eth,
      gross_dai
    from
      cover_data_expired
    where
      date_c >= NOW()
  ),
  starting_totals as (
    select
      1 as anchor,
      SUM(gross_eth) as eth,
      SUM(gross_dai) as dai
    from
      cover_data_summary
  )
SELECT
  *
from
  cover_expire_to_in_future
  JOIN starting_totals ON starting_totals.anchor = cover_expire_to_in_future.anchor