select
  --date_format(block_month, '%Y-%m') as block_month,
  block_month,
  -- === ETH ===
  -- capital pool total
  eth_capital_pool_start,
  eth_capital_pool_end,
  eth_capital_pool_pct,
  -- individual eth investments
  eth_steth,
  eth_steth_sale,
  eth_steth_return,
  eth_steth_apy,
  eth_reth,
  eth_reth_return,
  eth_reth_apy,
  eth_nxmty,
  eth_nxmty_return,
  eth_nxmty_apy,
  -- aave positions
  eth_aweth,
  eth_aweth_deposit,
  eth_aweth_withdraw,
  eth_aweth_return,
  eth_aweth_apy,
  eth_debt_usdc,
  eth_debt_usdc_borrow,
  eth_debt_usdc_repay,
  eth_debt_usdc_return,
  eth_debt_usdc_apy,
  eth_aave_net_return,
  eth_aave_net_apy,
  -- total eth investment returns
  eth_inv_returns,
  eth_inv_apy,
  -- fx impact
  eth_dai_fx_change,
  eth_usdc_fx_change,
  eth_cover_re_fx_change,
  eth_debt_usdc_fx_change,
  eth_fx_change,
  -- === USD ===
  -- capital pool total
  usd_capital_pool_start,
  usd_capital_pool_end,
  usd_capital_pool_pct,
  -- individual eth investments
  usd_steth,
  usd_steth_sale,
  usd_steth_return,
  usd_steth_apy,
  usd_reth,
  usd_reth_return,
  usd_reth_apy,
  usd_nxmty,
  usd_nxmty_return,
  usd_nxmty_apy,
  -- aave positions
  usd_aweth,
  usd_aweth_deposit,
  usd_aweth_withdraw,
  usd_aweth_return,
  usd_aweth_apy,
  usd_debt_usdc,
  usd_debt_usdc_borrow,
  usd_debt_usdc_repay,
  usd_debt_usdc_return,
  usd_debt_usdc_apy,
  usd_aave_net_return,
  usd_aave_net_apy,
  -- total eth investment returns
  usd_inv_returns,
  usd_inv_apy,
  -- fx impact
  usd_dai_fx_change,
  usd_usdc_fx_change,
  usd_cover_re_fx_change,
  usd_debt_usdc_fx_change,
  usd_fx_change
from query_4770697 -- investement returns
where block_month >= date_add('month', -12, current_date)
order by 1 desc
