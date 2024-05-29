with

capital_pool_latest as (
  select * from query_3774420 -- to be replaced with CP spell
),

capital_pool_pivot (asset, eth_total) as (
  select 'ETH', eth_total from capital_pool_latest union all
  select 'DAI', avg_dai_eth_total from capital_pool_latest union all
  select 'NXMTY', nxmty_eth_total from capital_pool_latest union all
  select 'stETH', steth_total from capital_pool_latest union all
  select 'rETH', avg_reth_eth_total from capital_pool_latest union all
  select 'USDC', avg_usdc_eth_total from capital_pool_latest union all
  select 'Cover Re USDC investment', avg_cover_re_usdc_eth_total from capital_pool_latest union all
  select 'Aave WETH collateral', aave_collateral_weth_total from capital_pool_latest union all
  select 'Aave USDC debt', avg_aave_debt_usdc_eth_total from capital_pool_latest
)

select *
from capital_pool_pivot
