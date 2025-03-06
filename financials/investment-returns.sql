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
    avg_dai_eth_total as dai,
    lead(avg_dai_eth_total, 1) over (order by block_date desc) as dai_prev,
    avg_usdc_eth_total as usdc,
    lead(eth_total, 1) over (order by block_date desc) as usdc_prev,
    avg_cbbtc_eth_total as cbbtc,
    lead(eth_total, 1) over (order by block_date desc) as cbbtc_prev,
    avg_cover_re_usdc_eth_total as cover_re,
    lead(eth_total, 1) over (order by block_date desc) as cover_re_prev,
    aave_collateral_weth_total as aweth,
    lead(eth_total, 1) over (order by block_date desc) as aweth_prev,
    avg_aave_debt_usdc_eth_total debt_usdc,
    lead(eth_total, 1) over (order by block_date desc) as debt_usdc_prev
  from nexusmutual_ethereum.capital_pool_totals
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
    (capital_pool - capital_pool_prev) / capital_pool_prev as capital_pool_pct,
    power(1 + ((capital_pool - capital_pool_prev) / capital_pool_prev), 12) - 1 as capital_pool_apy,
    eth,
    eth_prev,
    eth - eth_prev as eth_return,
    (eth - eth_prev) / eth_prev as eth_pct,
    power(1 + ((eth - eth_prev) / eth_prev), 12) - 1 as eth_apy,
    steth,
    steth_prev,
    steth - steth_prev as steth_return,
    (steth - steth_prev) / steth_prev as steth_pct,
    power(1 + ((steth - steth_prev) / steth_prev), 12) - 1 as steth_apy,
    reth,
    reth_prev,
    reth - reth_prev as reth_return,
    (reth - reth_prev) / reth_prev as reth_pct,
    power(1 + ((reth - reth_prev) / reth_prev), 12) - 1 as reth_apy,
    nxmty,
    nxmty_prev,
    nxmty - nxmty_prev as nxmty_return,
    (nxmty - nxmty_prev) / nxmty_prev as nxmty_pct,
    power(1 + ((nxmty - nxmty_prev) / nxmty_prev), 12) - 1 as nxmty_apy,
    dai,
    dai_prev,
    dai - dai_prev as dai_return,
    (dai - dai_prev) / dai_prev as dai_pct,
    power(1 + ((dai - dai_prev) / dai_prev), 12) - 1 as dai_apy,
    usdc,
    usdc_prev,
    usdc - usdc_prev as usdc_return,
    (usdc - usdc_prev) / usdc_prev as usdc_pct,
    power(1 + ((usdc - usdc_prev) / usdc_prev), 12) - 1 as usdc_apy,
    cbbtc,
    cbbtc_prev,
    cbbtc - cbbtc_prev as cbbtc_return,
    (cbbtc - cbbtc_prev) / cbbtc_prev as cbbtc_pct,
    power(1 + ((cbbtc - cbbtc_prev) / cbbtc_prev), 12) - 1 as cbbtc_apy,
    cover_re,
    cover_re_prev,
    cover_re - cover_re_prev as cover_re_return,
    (cover_re - cover_re_prev) / cover_re_prev as cover_re_pct,
    power(1 + ((cover_re - cover_re_prev) / cover_re_prev), 12) - 1 as cover_re_apy,
    aweth,
    aweth_prev,
    aweth - aweth_prev as aweth_return,
    (aweth - aweth_prev) / aweth_prev as aweth_pct,
    power(1 + ((aweth - aweth_prev) / aweth_prev), 12) - 1 as aweth_apy,
    debt_usdc,
    debt_usdc_prev,
    debt_usdc - debt_usdc_prev as debt_usdc_return,
    (debt_usdc - debt_usdc_prev) / debt_usdc_prev as debt_usdc_pct,
    power(1 + ((debt_usdc - debt_usdc_prev) / debt_usdc_prev), 12) - 1 as debt_usdc_apy
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
  (aweth_return + debt_usdc_return) / aweth_prev as aave_net_pct,
  power(1 + ((aweth_return + debt_usdc_return) / aweth_prev), 12) - 1 as aweth_apy,
  -- eth investment returns
  steth_return + reth_return + nxmty * (1-0.0015) + (aweth_return + debt_usdc_return) as eth_inv_returns,
  (steth_return + reth_return + nxmty * (1-0.0015) + (aweth_return + debt_usdc_return))
   / ((capital_pool_prev + capital_pool) / 2) as eth_inv_pct,
  power(1 + ((steth_return + reth_return + nxmty * (1-0.0015) + (aweth_return + debt_usdc_return))
   / ((capital_pool_prev + capital_pool) / 2)), 12) - 1 as eth_inv_apy,
  -- eth denominated
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
  -- stablecoin denominated
  dai,
  dai_return,
  dai_apy,
  usdc,
  usdc_return,
  usdc_apy,
  cover_re,
  cover_re_return,
  cover_re_apy,
  debt_usdc,
  debt_usdc_return,
  debt_usdc_apy
from investment_returns
order by 1 desc
