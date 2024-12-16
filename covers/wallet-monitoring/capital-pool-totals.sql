select
  block_date,
  avg_eth_usd_price,
  avg_capital_pool_eth_total,
  avg_capital_pool_usd_total,
  current_timestamp as inserted_at
from nexusmutual_ethereum.capital_pool_totals
where block_date >= timestamp '2024-12-01'
order by 1
