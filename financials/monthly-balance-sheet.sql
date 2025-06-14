select
  --date_format(block_month, '%Y-%m') as block_month,
  block_month,
  eth_capital_pool,
  eth_eth + eth_steth + eth_reth + eth_cbbtc + eth_nxmty + eth_aweth as eth_crypto_assets,
  eth_eth,
  eth_steth,
  eth_reth,
  eth_nxmty,
  eth_aweth,
  eth_dai + eth_usdc + eth_cover_re + eth_debt_usdc as eth_stablecoin_assets,
  eth_debt_usdc,
  eth_dai,
  eth_usdc,
  eth_cbbtc,
  eth_cover_re,
  usd_capital_pool,
  usd_eth + usd_steth + usd_reth + usd_cbbtc + usd_nxmty + usd_aweth as usd_crypto_assets,
  usd_eth,
  usd_steth,
  usd_reth,
  usd_nxmty,
  usd_aweth,
  usd_dai + usd_usdc + usd_cover_re + usd_debt_usdc as usd_stablecoin_assets,
  usd_debt_usdc,
  usd_dai,
  usd_usdc,
  usd_cbbtc,
  usd_cover_re
from query_4841979 -- balance sheet
where block_month >= date_add('month', -12, date_trunc('month', current_date))
  and block_month < date_trunc('month', current_date)
order by 1 desc
