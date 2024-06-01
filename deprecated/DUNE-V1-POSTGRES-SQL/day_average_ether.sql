

# Average day price of ether, parameterised over 

   select date_trunc('day', minute) as day, avg(price) as avg_price
        from prices."layer1_usd"
        where symbol = 'ETH' AND minute >= '{{start_date}}' AND  minute <= '{{end_date}}'
        GROUP BY day
        ORDER BY day DESC

# SMA over 7 days as snippet

  ethereum_price_average as (
    select
      date_trunc('day', minute) as day,
      avg(price) as eth_avg_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
    GROUP BY
      day
    ORDER BY
      day
  ),
  ethereum_price_ma7 as (
    select
      day,
      eth_avg_price,
      avg(eth_avg_price) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as eth_moving_average
    from
      ethereum_price_average
    ORDER BY
      day DESC
  ),



SELECT 
*,
  SUM(
    dai_ingress + dai_egress + ((lido_ingress - lido_egress) * eth_moving_average) + (
      (total_eth_ingress - total_eth_egress) * eth_moving_average
    )
  ) as dollar_total
FROM



