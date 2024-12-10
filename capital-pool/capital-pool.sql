with

capital_pool as (
  select *
  --from query_3773633 -- Capital Pool base (fallback) query
  from nexusmutual_ethereum.capital_pool_totals
)

select
  block_date,
  avg_eth_usd_price,
  if('{{display_currency}}' = 'USD', avg_capital_pool_usd_total, avg_capital_pool_eth_total) as capital_pool_display_curr,
  if('{{display_currency}}' = 'USD', avg_eth_usd_total, eth_total) as eth_display_curr,
  if('{{display_currency}}' = 'USD', avg_dai_usd_total, avg_dai_eth_total) as dai_display_curr,
  if('{{display_currency}}' = 'USD', avg_nxmty_usd_total, nxmty_eth_total) as nxmty_display_curr,
  if('{{display_currency}}' = 'USD', avg_steth_usd_total, steth_total) as steth_display_curr,
  if('{{display_currency}}' = 'USD', avg_reth_usd_total, avg_reth_eth_total) as reth_display_curr,
  if('{{display_currency}}' = 'USD', avg_usdc_usd_total, avg_usdc_eth_total) as usdc_display_curr,
  if('{{display_currency}}' = 'USD', avg_cbbtc_usd_total, avg_cbbtc_eth_total) as cbbtc_display_curr,
  if('{{display_currency}}' = 'USD', avg_cover_re_usdc_usd_total, avg_cover_re_usdc_eth_total) as cover_re_usdc_display_curr,
  if('{{display_currency}}' = 'USD', avg_aave_collateral_weth_usd_total, aave_collateral_weth_total) as aave_collateral_weth_display_curr,
  if('{{display_currency}}' = 'USD', avg_aave_debt_usdc_usd_total, avg_aave_debt_usdc_eth_total) as aave_debt_usdc_display_curr
from capital_pool
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
order by 1 desc
