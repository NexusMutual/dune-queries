with capital_pool as (
  select
    block_date,
    avg_capital_pool_eth_total as capital_pool_eth,
    eth_total,
    steth_total,
    avg_reth_eth_total as reth,
    lead(avg_reth_eth_total, 1) over (order by block_date desc) as reth_prev,
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
  limit 12 -- rolling 12 months
)

select
  block_date,
  reth,
  reth_prev,
  reth - reth_prev as reth_return,
  (reth - reth_prev) / reth_prev as reth_pct,
  power(1 + ((reth - reth_prev) / reth_prev), 12) - 1 as reth_apy
from capital_pool
order by 1 desc
