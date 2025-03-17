with

capital_pool as (
  select
    date_trunc('month', block_date) as block_month,
    eth_capital_pool,
    lag(eth_capital_pool, 1) over (order by block_date) as eth_capital_pool_prev,
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
    lag(usd_capital_pool, 1) over (order by block_date) as usd_capital_pool_prev,
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
  from (
    select
      block_date,
      avg_capital_pool_eth_total as eth_capital_pool,
      eth_total as eth_eth,
      steth_total as eth_steth,
      avg_reth_eth_total as eth_reth,
      nxmty_eth_total as eth_nxmty,
      aave_collateral_weth_total as eth_aweth,
      avg_aave_debt_usdc_eth_total as eth_debt_usdc,
      avg_dai_eth_total as eth_dai,
      avg_usdc_eth_total as eth_usdc,
      avg_cbbtc_eth_total as eth_cbbtc,
      avg_cover_re_usdc_eth_total as eth_cover_re,
      avg_capital_pool_usd_total as usd_capital_pool,
      avg_eth_usd_total as usd_eth,
      avg_steth_usd_total as usd_steth,
      avg_reth_usd_total as usd_reth,
      avg_nxmty_usd_total as usd_nxmty,
      avg_aave_collateral_weth_usd_total as usd_aweth,
      avg_aave_debt_usdc_usd_total as usd_debt_usdc,
      avg_dai_usd_total as usd_dai,
      avg_usdc_usd_total as usd_usdc,
      avg_cbbtc_usd_total as usd_cbbtc,
      avg_cover_re_usdc_usd_total as usd_cover_re,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from query_4627588 -- Capital Pool - base root
  ) t
  where t.rn = 1
  order by 1 desc
  limit 12 -- 12 months rolling
)

select
  --date_format(block_month, '%Y-%m') as block_month,
  block_month,
  eth_capital_pool,
  eth_capital_pool_prev,
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
  usd_capital_pool_prev,
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
order by 1 desc
