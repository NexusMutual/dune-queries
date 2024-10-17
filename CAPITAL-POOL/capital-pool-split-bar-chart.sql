with

capital_pool_timed as (
  select *
  from query_3773633 -- Capital Pool base (fallback) query
  --from nexusmutual_ethereum.capital_pool_totals
  where block_date = if(timestamp '{{End Date}}' > current_timestamp, cast(current_date as timestamp), timestamp '{{End Date}}')
),

capital_pool_assets (asset_type, eth_amount, usd_amount, rn) as (
  select 'ETH', eth_total, avg_eth_usd_total, 1 from capital_pool_timed union all
  select 'DAI', avg_dai_eth_total, avg_dai_usd_total, 2 from capital_pool_timed union all
  select 'stETH', steth_total, avg_steth_usd_total, 3 from capital_pool_timed union all
  select 'rETH', avg_reth_eth_total, avg_reth_usd_total, 4 from capital_pool_timed union all
  select 'NXMTY', nxmty_eth_total, avg_nxmty_usd_total, 5 from capital_pool_timed union all
  select 'USDC', avg_usdc_eth_total, avg_usdc_usd_total, 6 from capital_pool_timed union all
  select 'Cover Re USDC', avg_cover_re_usdc_eth_total, avg_cover_re_usdc_usd_total, 7 from capital_pool_timed union all
  select 'Aave collateral WETH', aave_collateral_weth_total, avg_aave_collateral_weth_usd_total, 8 from capital_pool_timed union all
  select 'Aave debt USDC', avg_aave_debt_usdc_eth_total, avg_aave_debt_usdc_usd_total, 9 from capital_pool_timed
)

select
  asset_type,
  if('{{display_currency}}' = 'USD', usd_amount, eth_amount) as asset_value
from capital_pool_assets
order by asset_value desc
