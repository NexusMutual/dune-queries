with

items as (
  select fi_id, label, label_tab
  from query_4832890 -- fin items
  where scope = 'bs'
),

balance_sheet as (
  select
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
  from query_4841979 -- balance sheet - base
  where block_month = timestamp '2025-02-01'
)

select
  i.label_tab,
  case i.label
    when 'Balance Sheet' then null
    when 'Crypto Denominated Assets' then eth_eth + eth_steth + eth_reth + eth_cbbtc + eth_nxmty + eth_aweth
    when 'ETH' then eth_eth
    when 'stETH' then eth_steth
    when 'rETH' then eth_reth
    when 'cbBTC' then eth_cbbtc
    when 'Enzyme Vault' then eth_nxmty
    when 'Aave aEthWETH' then eth_aweth
    when 'Stablecoin Denominated Assets' then eth_dai + eth_usdc + eth_cover_re + eth_debt_usdc
    when 'DAI' then eth_dai
    when 'USDC' then eth_usdc
    when 'Cover Re' then eth_cover_re
    when 'Aave debtUSDC' then eth_debt_usdc
    when 'Total Balance' then eth_capital_pool
  end as eth_val,
  case i.label
    when 'Balance Sheet' then null
    when 'Crypto Denominated Assets' then usd_eth + usd_steth + usd_reth + usd_cbbtc + usd_nxmty + usd_aweth
    when 'ETH' then usd_eth
    when 'stETH' then usd_steth
    when 'rETH' then usd_reth
    when 'cbBTC' then usd_cbbtc
    when 'Enzyme Vault' then usd_nxmty
    when 'Aave aEthWETH' then usd_aweth
    when 'Stablecoin Denominated Assets' then usd_dai + usd_usdc + usd_cover_re + usd_debt_usdc
    when 'DAI' then usd_dai
    when 'USDC' then usd_usdc
    when 'Cover Re' then usd_cover_re
    when 'Aave debtUSDC' then usd_debt_usdc
    when 'Total Balance' then usd_capital_pool
  end as usd_val
from balance_sheet bs
  cross join items i
order by i.fi_id
