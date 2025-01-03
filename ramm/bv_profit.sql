with

prices as (
  select
    date_trunc('minute', minute) as block_minute,
    avg(price) as avg_eth_usd_price
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and minute > cast('2023-11-11' as timestamp)
  group by 1
),

bv_diff_nxm_eth_swap as (
  select
    date_trunc('minute', r.evt_block_time) as block_minute,
    s.output_0 / 1e18 as nxm_supply_pre_sale,
    c.output_0 / 1e18 as eth_capital_pool_pre_sale,
    (s.output_0 - r.nxmIn) / 1e18 as nxm_supply_post_sale,
    (c.output_0 - r.ethOut) / 1e18 as eth_capital_pool_post_sale
  from nexusmutual_ethereum.Ramm_evt_NxmSwappedForEth r
    inner join nexusmutual_ethereum.Pool_call_getPoolValueInEth c on r.evt_block_time = c.call_block_time and r.evt_tx_hash = c.call_tx_hash
    inner join nexusmutual_ethereum.NXMToken_call_totalSupply s on c.call_block_time = s.call_block_time and c.call_tx_hash = s.call_tx_hash
  where c.call_success
    and s.call_success
),

bv_diff_nxm_eth_swap_ext as (
  select
    block_minute,
    nxm_supply_pre_sale,
    eth_capital_pool_pre_sale,
    nxm_supply_post_sale,
    eth_capital_pool_post_sale,
    (eth_capital_pool_post_sale / nxm_supply_post_sale) - (eth_capital_pool_pre_sale / nxm_supply_pre_sale) as bv_diff_per_nxm,
    ((eth_capital_pool_post_sale / nxm_supply_post_sale) - (eth_capital_pool_pre_sale / nxm_supply_pre_sale)) * nxm_supply_post_sale as bv_diff
  from bv_diff_nxm_eth_swap
),

bv_profit as (
  select
    s.block_minute,
    s.bv_diff_per_nxm,
    s.bv_diff,
    sum(s.bv_diff) over (order by s.block_minute) as bv_diff_cummulative_eth,
    sum(s.bv_diff * p.avg_eth_usd_price) over (order by s.block_minute) as bv_diff_cummulative_usd
  from bv_diff_nxm_eth_swap_ext s
    inner join prices p on s.block_minute = p.block_minute
)

select
  block_minute,
  bv_diff_per_nxm,
  bv_diff,
  bv_diff_cummulative_eth,
  bv_diff_cummulative_usd,
  if('{{currency}}' = 'USD', bv_diff_cummulative_usd, bv_diff_cummulative_eth) as bv_diff_cummulative
from bv_profit
order by 1 desc
