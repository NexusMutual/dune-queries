

# Average day price of ether, parameterised over 

   select date_trunc('day', minute) as day, avg(price) as avg_price
        from prices."layer1_usd"
        where symbol = 'ETH' AND minute >= '{{start_date}}' AND  minute <= '{{end_date}}'
        GROUP BY day
        ORDER BY day DESC


# SMA oover 7 days,


 with
  ethereum_price_ma6 as (
    select
      date_trunc('day', minute) as day,
      avg(price) as avg_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
    GROUP BY
      day
    ORDER BY
      day
  )
select
  day,
  avg_price,
  avg(avg_price) OVER (
    ORDER BY
      day ROWS BETWEEN 6 PRECEDING
      AND CURRENT ROW
  ) as moving_average
from
  ethereum_price_ma90
ORDER BY
      day DESC