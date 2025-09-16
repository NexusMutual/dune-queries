select
  block_date,
  avg_eth_usd_price,
  avg_capital_pool_eth_total,
  avg_capital_pool_usd_total,
  current_timestamp as inserted_at
from query_4627588 -- Capital Pool - base root
where block_date >= timestamp '2024-12-01'
order by 1
