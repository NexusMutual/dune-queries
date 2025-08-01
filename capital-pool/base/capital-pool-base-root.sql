select
  block_date,
  avg_eth_usd_price,
  avg_dai_usd_price,
  avg_usdc_usd_price,
  avg_cbbtc_usd_price,
  -- Capital Pool totals
  avg_capital_pool_eth_total,
  avg_capital_pool_usd_total,
  -- ETH
  eth_total,
  avg_eth_usd_total,
  -- DAI
  if(avg_dai_eth_total > 0, dai_total, 0) as dai_total,
  if(avg_dai_eth_total > 0, avg_dai_usd_total, 0) as avg_dai_usd_total,
  if(avg_dai_eth_total > 0, avg_dai_eth_total, 0) as avg_dai_eth_total,
  -- NXMTY
  nxmty_total,
  nxmty_eth_total,
  avg_nxmty_usd_total,
  -- stETH
  steth_total,
  avg_steth_usd_total,
  -- rETH
  reth_total,
  avg_reth_usd_total,
  avg_reth_eth_total,
  -- USDC
  usdc_total,
  avg_usdc_usd_total,
  avg_usdc_eth_total,
  -- cbBTC
  cbbtc_total,
  avg_cbbtc_usd_total,
  avg_cbbtc_eth_total,
  -- Cover Re USDC investment
  cover_re_usdc_total,
  avg_cover_re_usdc_usd_total,
  avg_cover_re_usdc_eth_total,
  -- AAVE positions
  aave_collateral_weth_total,
  avg_aave_collateral_weth_usd_total,
  aave_debt_usdc_total,
  avg_aave_debt_usdc_usd_total,
  avg_aave_debt_usdc_eth_total
--from query_3773633 -- Capital Pool base (fallback query)
from nexusmutual_ethereum.capital_pool_totals
