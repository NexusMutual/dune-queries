select
  --date_format(block_month, '%Y-%m') as block_month,
  block_month,
  eth_capital_pool,
  eth_eth,
  eth_steth,
  eth_reth,
  eth_nxmty,
  eth_aweth,
  eth_debt_usdc,
  eth_dai,
  eth_usdc,
  eth_cbbtc,
  eth_cover_re,
  usd_capital_pool,
  usd_eth,
  usd_steth,
  usd_reth,
  usd_nxmty,
  usd_aweth,
  usd_debt_usdc,
  usd_dai,
  usd_usdc,
  usd_cbbtc,
  usd_cover_re
from capital_pool
where block_month >= date_add('month', -12, current_date)
order by 1 desc
