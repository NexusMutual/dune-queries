with
  cover_data as (
    select
      1 as anchor,
      "cid" as cover_id,
      "evt_block_time" as cover_start_time,
      "sumAssured" as sum_assured,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      case
        when "curr" = '\x45544800' then "sumAssured"
        ELSE 0
      end as gross_eth,
      case
        when "curr" = '\x44414900' then "sumAssured"
        ELSE 0
      end as gross_dai,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as cover_end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
  ),
  ethereum_price as (
    select
      1 as anchor,
      date_trunc('day', minute) as day,
      avg(price) as eth_avg_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
      and date_trunc('day', minute) < NOW()
    GROUP BY
      day
    ORDER BY
      day DESC
    LIMIT
      1
  ), weigthed_cal as (
    SELECT
      eth_avg_price * gross_eth as dollar_eth,
      (eth_avg_price * gross_eth) + gross_dai as dollar,
      (cover_end_time - NOW()) :: interval as diff,
      cover_end_time,
      NOW(),
      date_part('days', (cover_end_time - NOW()) :: interval) * ((eth_avg_price * gross_eth) + gross_dai) as day_weighted_dollar
    FROM
      cover_data
      JOIN ethereum_price ON ethereum_price.anchor = cover_data.anchor
    WHERE
      cover_end_time > NOW()
  )
select
  SUM(day_weighted_dollar) / SUM(dollar) as total_dollar_weighted
FROM
  weigthed_cal