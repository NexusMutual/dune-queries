with

historical_prices as (
  select
    date_trunc('minute', minute) as block_minute,
    avg(price) as avg_eth_usd_price
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute > cast('2023-11-11' as timestamp)
  group by 1
),

latest_prices as (
  select
    minute as block_minute,
    symbol,
    price as usd_price
  from prices.usd_latest
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
  order by 1 desc
  limit 1
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
    -- helpers
    (eth_capital_pool_pre_sale / nxm_supply_pre_sale) as bv_per_nxm_pre_eth,
    (eth_capital_pool_post_sale / nxm_supply_post_sale) as bv_per_nxm_post_eth,
    -- in ETH
    (eth_capital_pool_post_sale / nxm_supply_post_sale) - (eth_capital_pool_pre_sale / nxm_supply_pre_sale) as bv_diff_per_nxm_in_eth,
    ((eth_capital_pool_post_sale / nxm_supply_post_sale) - (eth_capital_pool_pre_sale / nxm_supply_pre_sale)) * nxm_supply_post_sale as bv_diff_eth,
    -- in NXM
    (((eth_capital_pool_post_sale / nxm_supply_post_sale) - (eth_capital_pool_pre_sale / nxm_supply_pre_sale)) 
      / (eth_capital_pool_post_sale / nxm_supply_post_sale)) as bv_diff_per_nxm_in_nxm,
    ( ((eth_capital_pool_post_sale / nxm_supply_post_sale) - (eth_capital_pool_pre_sale / nxm_supply_pre_sale)) * nxm_supply_post_sale )
      / (eth_capital_pool_post_sale / nxm_supply_post_sale) as bv_diff_nxm
  from bv_diff_nxm_eth_swap
),

bv_profit as (
  select
    s.block_minute,
    s.bv_diff_per_nxm_in_eth,
    s.bv_diff_eth,
    s.bv_diff_per_nxm_in_nxm,
    s.bv_diff_nxm,
    s.bv_diff_eth * p.avg_eth_usd_price as bv_diff_usd,
    s.bv_diff_eth * lp.usd_price as bv_diff_usd_latest
  from bv_diff_nxm_eth_swap_ext s
    inner join historical_prices p on s.block_minute = p.block_minute
    cross join latest_prices lp
),

daily as (
  select
    date_trunc('day', block_minute) as block_date,
    sum(bv_diff_eth) as bv_diff_eth,
    sum(bv_diff_nxm) as bv_diff_nxm,
    sum(bv_diff_usd) as bv_diff_usd,
    sum(bv_diff_usd_latest) as bv_diff_usd_latest,
    sum(bv_diff_per_nxm_in_eth) as bv_diff_per_nxm_in_eth,
    sum(bv_diff_per_nxm_in_nxm) as bv_diff_per_nxm_in_nxm
  from bv_profit
  group by 1
),

daily_incl_cummulative as (
  select
    block_date,
    bv_diff_eth,
    bv_diff_nxm,
    bv_diff_usd,
    bv_diff_usd_latest,
    bv_diff_per_nxm_in_eth,
    bv_diff_per_nxm_in_nxm,
    sum(bv_diff_eth) over (order by block_date) as bv_diff_eth_cummulative,
    sum(bv_diff_nxm) over (order by block_date) as bv_diff_nxm_cummulative,
    sum(bv_diff_usd) over (order by block_date) as bv_diff_usd_cummulative,
    sum(bv_diff_usd_latest) over (order by block_date) as bv_diff_usd_latest_cummulative
  from daily
)

select
  block_date,
  bv_diff_eth,
  bv_diff_nxm,
  bv_diff_usd,
  bv_diff_per_nxm_in_eth,
  bv_diff_per_nxm_in_nxm,
  bv_diff_eth_cummulative,
  bv_diff_nxm_cummulative,
  bv_diff_usd_cummulative,
  bv_diff_usd_latest,
  bv_diff_usd_latest_cummulative,
  avg(bv_diff_eth) over (
    order by block_date
    rows between 2 preceding and current row
  ) as bv_diff_3m_moving_avg_eth,
  avg(bv_diff_nxm) over (
    order by block_date
    rows between 2 preceding and current row
  ) as bv_diff_3m_moving_avg_nxm,
  avg(bv_diff_usd) over (
    order by block_date
    rows between 2 preceding and current row
  ) as bv_diff_3m_moving_avg_usd
from daily_incl_cummulative
where block_date >= timestamp '2024-01-01'
order by 1
