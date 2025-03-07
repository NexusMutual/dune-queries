with

capital_pool as (
  select
    block_date,
    avg_capital_pool_eth_total as capital_pool,
    lead(avg_capital_pool_eth_total, 1) over (order by block_date desc) as capital_pool_prev,
    eth_total as eth,
    lead(eth_total, 1) over (order by block_date desc) as eth_prev,
    steth_total as steth,
    lead(steth_total, 1) over (order by block_date desc) as steth_prev,
    avg_reth_eth_total as reth,
    lead(avg_reth_eth_total, 1) over (order by block_date desc) as reth_prev,
    nxmty_eth_total as nxmty,
    lead(nxmty_eth_total, 1) over (order by block_date desc) as nxmty_prev,
    avg_cbbtc_eth_total as cbbtc,
    lead(avg_cbbtc_eth_total, 1) over (order by block_date desc) as cbbtc_prev,
    aave_collateral_weth_total as aweth,
    lead(aave_collateral_weth_total, 1) over (order by block_date desc) as aweth_prev,
    avg_aave_debt_usdc_eth_total debt_usdc,
    lead(avg_aave_debt_usdc_eth_total, 1) over (order by block_date desc) as debt_usdc_prev
  from query_4627588 -- Capital Pool - base root
  where day(block_date) = 1
  order by 1 desc
  limit 12 -- 12 months rolling
),

investment_returns as (
  select
    block_date,
    capital_pool,
    capital_pool_prev,
    capital_pool - capital_pool_prev as capital_pool_return,
    coalesce((capital_pool - capital_pool_prev) / nullif(capital_pool_prev, 0), 0) as capital_pool_pct,
    coalesce(power(1 + ((capital_pool - capital_pool_prev) / nullif(capital_pool_prev, 0)), 12) - 1, 0) as capital_pool_apy,
    eth,
    eth_prev,
    eth - eth_prev as eth_return,
    coalesce((eth - eth_prev) / nullif(eth_prev, 0), 0) as eth_pct,
    coalesce(power(1 + ((eth - eth_prev) / nullif(eth_prev, 0)), 12) - 1, 0) as eth_apy,
    steth,
    steth_prev,
    steth - steth_prev as steth_return,
    coalesce((steth - steth_prev) / nullif(steth_prev, 0), 0) as steth_pct,
    coalesce(power(1 + ((steth - steth_prev) / nullif(steth_prev, 0)), 12) - 1, 0) as steth_apy,
    reth,
    reth_prev,
    reth - reth_prev as reth_return,
    coalesce((reth - reth_prev) / nullif(reth_prev, 0), 0) as reth_pct,
    coalesce(power(1 + ((reth - reth_prev) / nullif(reth_prev, 0)), 12) - 1, 0) as reth_apy,
    nxmty,
    nxmty_prev,
    nxmty - nxmty_prev as nxmty_return,
    coalesce((nxmty - nxmty_prev) / nullif(nxmty_prev, 0), 0) as nxmty_pct,
    coalesce(power(1 + ((nxmty - nxmty_prev) / nullif(nxmty_prev, 0)), 12) - 1, 0) as nxmty_apy,
    cbbtc,
    cbbtc_prev,
    cbbtc - cbbtc_prev as cbbtc_return,
    coalesce((cbbtc - cbbtc_prev) / nullif(cbbtc_prev, 0), 0) as cbbtc_pct,
    coalesce(power(1 + ((cbbtc - cbbtc_prev) / nullif(cbbtc_prev, 0)), 12) - 1, 0) as cbbtc_apy,
    aweth,
    aweth_prev,
    aweth - aweth_prev as aweth_return,
    coalesce((aweth - aweth_prev) / nullif(aweth_prev, 0), 0) as aweth_pct,
    coalesce(power(1 + ((aweth - aweth_prev) / nullif(aweth_prev, 0)), 12) - 1, 0) as aweth_apy,
    debt_usdc,
    debt_usdc_prev,
    debt_usdc - debt_usdc_prev as debt_usdc_return,
    coalesce((debt_usdc - debt_usdc_prev) / nullif(debt_usdc_prev, 0), 0) as debt_usdc_pct,
    coalesce(power(1 + ((debt_usdc - debt_usdc_prev) / nullif(debt_usdc_prev, 0)), 12) - 1, 0) as debt_usdc_apy
  from capital_pool
)

select
  block_date,
  -- capital pool total
  capital_pool,
  capital_pool_return,
  capital_pool_apy,
  -- aave net return
  aweth_return + debt_usdc_return as aave_net_return,
  coalesce(power(1 + ((aweth_return + debt_usdc_return) / nullif(aweth_prev, 0)), 12) - 1, 0) as aweth_net_apy,
  -- eth investment returns
  steth_return + reth_return + nxmty * (1-0.0015) + (aweth_return + debt_usdc_return) as eth_inv_returns,
  coalesce(power(1 + ((steth_return + reth_return + nxmty * (1-0.0015) + (aweth_return + debt_usdc_return))
   / nullif(((capital_pool_prev + capital_pool) / 2), 0)), 12) - 1, 0) as eth_inv_apy,
  -- investments
  eth,
  eth_return,
  eth_apy,
  steth,
  steth_return,
  steth_apy,
  reth,
  reth_return,
  reth_apy,
  nxmty * (1-0.0015) as nxmty, -- minus Enxyme fee
  nxmty_return,
  nxmty_apy,
  cbbtc,
  cbbtc_return,
  cbbtc_apy,
  aweth,
  aweth_return,
  aweth_apy,
  debt_usdc,
  debt_usdc_return,
  debt_usdc_apy
from investment_returns
order by 1 desc
