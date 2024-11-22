WITH
  nxm_allocated_stake as (
    select
      date_trunc('day', evt_block_time) as day,
      amount * 1E-18 as nxm_amount,
      "contractAddress" as product_address
    from
      nexusmutual."PooledStaking_evt_Staked"
    UNION
    select
      date_trunc('day', evt_block_time) as day,
      amount * -1E-18 as nxm_amount,
      "contractAddress" as product_address
    from
      nexusmutual."PooledStaking_evt_Unstaked"
  ),
  average_day_ethereum_price as (
    select
      CAST(date_trunc('day', minute) as DATE) as day,
      avg(price) as avg_price
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
      avg(price) as avg_price
    from
      prices."usd"
    where
      symbol = 'DAI'
    GROUP BY
      day
    ORDER BY
      day
  ),
  ethereum_price_ma7 as (
    select
      day,
      avg_price,
      avg(avg_price) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_eth
    from
      average_day_ethereum_price
    ORDER BY
      day DESC
  ),
  dai_price_ma7 as (
    select
      day,
      avg_price,
      avg(avg_price) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_dai
    from
      average_day_dai_price
    ORDER BY
      day DESC
  ),
  price_ma as (
    select
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai
    from
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
  )
select
  distinct day,
  SUM(nxm_amount) OVER (PARTITION BY day) as net_change,
  SUM(nxm_amount) OVER (
    ORDER BY
      day
  ) as running_total
from
  nxm_allocated_stake
WHERE
  day >= '{{1. Start Date}}'
  AND day <= '{{2. End Date}}'
ORDER BY
  day DESC