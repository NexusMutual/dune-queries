with

params as (
  select cast(case '{{month}}'
      when 'current month' then cast(date_trunc('month', current_date) as varchar)
      when 'last month' then cast(date_add('month', -1, date_trunc('month', current_date)) as varchar)
      else '{{month}}'
    end as timestamp) as report_month
),

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
  where block_month in (select report_month from params)
)

select
  i.label_tab,
  case i.label
    when 'Balance Sheet' then null
    when 'Crypto Denominated Assets' then eth_eth + eth_steth + eth_reth + eth_cbbtc + eth_nxmty + eth_aweth
    when 'ETH' then coalesce(nullif(eth_eth, 0), 1e-6)
    when 'stETH' then coalesce(nullif(eth_steth, 0), 1e-6)
    when 'rETH' then coalesce(nullif(eth_reth, 0), 1e-6)
    when 'cbBTC' then coalesce(nullif(eth_cbbtc, 0), 1e-6)
    when 'Enzyme Vault' then coalesce(nullif(eth_nxmty, 0), 1e-6)
    when 'Aave aEthWETH' then coalesce(nullif(eth_aweth, 0), 1e-6)
    when 'Stablecoin Denominated Assets' then eth_dai + eth_usdc + eth_cover_re + eth_debt_usdc
    when 'DAI' then coalesce(nullif(eth_dai, 0), 1e-6)
    when 'USDC' then coalesce(nullif(eth_usdc, 0), 1e-6)
    when 'Cover Re' then coalesce(nullif(eth_cover_re, 0), 1e-6)
    when 'Aave debtUSDC' then coalesce(nullif(eth_debt_usdc, 0), 1e-6)
    when 'Total Balance' then coalesce(nullif(eth_capital_pool, 0), 1e-6)
  end as eth_val,
  case i.label
    when 'Balance Sheet' then null
    when 'Crypto Denominated Assets' then usd_eth + usd_steth + usd_reth + usd_cbbtc + usd_nxmty + usd_aweth
    when 'ETH' then coalesce(nullif(usd_eth, 0), 1e-6)
    when 'stETH' then coalesce(nullif(usd_steth, 0), 1e-6)
    when 'rETH' then coalesce(nullif(usd_reth, 0), 1e-6)
    when 'cbBTC' then coalesce(nullif(usd_cbbtc, 0), 1e-6)
    when 'Enzyme Vault' then coalesce(nullif(usd_nxmty, 0), 1e-6)
    when 'Aave aEthWETH' then coalesce(nullif(usd_aweth, 0), 1e-6)
    when 'Stablecoin Denominated Assets' then usd_dai + usd_usdc + usd_cover_re + usd_debt_usdc
    when 'DAI' then coalesce(nullif(usd_dai, 0), 1e-6)
    when 'USDC' then coalesce(nullif(usd_usdc, 0), 1e-6)
    when 'Cover Re' then coalesce(nullif(usd_cover_re, 0), 1e-6)
    when 'Aave debtUSDC' then coalesce(nullif(usd_debt_usdc, 0), 1e-6)
    when 'Total Balance' then coalesce(nullif(usd_capital_pool, 0), 1e-6)
  end as usd_val
from balance_sheet bs
  cross join items i
order by i.fi_id
