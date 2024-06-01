with av_dollar_price_dai as (
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



