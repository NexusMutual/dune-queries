with

prices as (
  select
    minute as block_minute,
    price as eth_usd_price
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and minute > cast('2023-11-11' as timestamp)
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
    sum(s.bv_diff * p.eth_usd_price) over (order by s.block_minute) as bv_diff_cummulative_usd
  from bv_diff_nxm_eth_swap_ext s
    inner join prices p on s.block_minute = p.block_minute
)

select
  date_trunc('month', block_minute) as block_month,
  sum(bv_diff_per_nxm) as bv_profit_per_nxm,
  sum(bv_diff) as bv_profit_eth
from bv_profit
group by 1
order by 1 desc
