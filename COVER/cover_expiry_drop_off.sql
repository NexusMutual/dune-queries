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
      end as gross_eth_expired,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then -1 * sum_assured
      end as gross_dai_expired,
      date_trunc('day', cover_end_time) as date_c
    from
      cover_data as s
  ),
  cover_data_buy as (
    select
      cover_asset,
      case
        when cover_asset = 'ETH' then sum_assured
        when cover_asset = 'DAI' then 0
      end as gross_eth_brought,
      case
        when cover_asset = 'ETH' then 0
        when cover_asset = 'DAI' then sum_assured
      end as gross_dai_brought,
      date_trunc('day', cover_start_time) as date_c
    from
      cover_data
  ),
  cover_data_buy_summed as (
    select
      date_c,
      SUM(gross_eth_brought) as eth_summed_buy,
      SUM(gross_dai_brought) as dai_summed_buy
    FROM
      cover_data_buy
    group by
      date_c
  ),
  cover_data_sold_summed as (
    select
      date_c,
      SUM(gross_eth_expired) as eth_summed_expired,
      SUM(gross_dai_expired) as dai_summed_expired
    FROM
      cover_data_expired
    WHERE
      date_c < NOW()
    group by
      date_c
  ),
  cover_day_changes as (
    SELECT
      cover_data_buy_summed.date_c,
      cover_data_buy_summed.eth_summed_buy,
      cover_data_sold_summed.eth_summed_expired,
      cover_data_buy_summed.dai_summed_buy,
      cover_data_sold_summed.dai_summed_expired,
      cover_data_buy_summed.eth_summed_buy + cover_data_sold_summed.eth_summed_expired as net_eth,
      cover_data_buy_summed.dai_summed_buy + cover_data_sold_summed.dai_summed_expired as net_dai
    from
      cover_data_sold_summed
      JOIN cover_data_buy_summed ON cover_data_sold_summed.date_c = cover_data_buy_summed.date_c
  ),
  starting_totals as (
    select
      1 as anchor,
      sum(net_eth) as starting_eth,
      sum(net_dai) as starting_dai
    FROM
      cover_day_changes
  ),
  cover_expire_to_in_future as (
    SELECT
      DISTINCT date_c,
      1 as anchor,
      SUM(gross_eth_expired) OVER (
        ORDER BY
          date_c ASC
      ) as eth_running_expiry,
      SUM(gross_dai_expired) OVER (
        ORDER BY
          date_c ASC
      ) as dai_running_expiry
    from
      cover_data_expired
    where
      date_c >= NOW()
  )
SELECT
  date_c,
  eth_running_expiry,
  dai_running_expiry,
  starting_eth,
  starting_dai,
  starting_eth + eth_running_expiry as total_eth,
  starting_dai + dai_running_expiry as total_dai
from
  cover_expire_to_in_future
  JOIN starting_totals ON starting_totals.anchor = cover_expire_to_in_future.anchor