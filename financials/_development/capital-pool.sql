select
  block_date,
  avg_capital_pool_eth_total,
  eth_total,
  steth_total,
  avg_reth_eth_total,
  nxmty_eth_total,
  avg_dai_eth_total,
  avg_usdc_eth_total,
  avg_cbbtc_eth_total,
  avg_cover_re_usdc_eth_total,
  aave_collateral_weth_total,
  avg_aave_debt_usdc_eth_total
from nexusmutual_ethereum.capital_pool_totals
where day(block_date) = 1
order by 1 desc
limit 10
