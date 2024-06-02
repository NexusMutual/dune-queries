with

capital_pool as (
  select
    block_date,
    avg_eth_usd_price,
    avg_capital_pool_eth_total  
  from query_3773633 -- Capital Pool base (fallback) query
  where block_date >= timestamp '2019-11-06'
),

mcr as (
  select
    block_date,
    mcr_eth_total
  from query_3787908 -- MCR base (fallback) query
),

nxm_daily_price_pre_ramm as (
  select
    cp.block_date,
    cp.avg_eth_usd_price,
    cp.avg_capital_pool_eth_total,
    mcr.mcr_eth_total,
    cast(
      0.01028 + (mcr.mcr_eth_total / 5800000) * power((cp.avg_capital_pool_eth_total / mcr.mcr_eth_total), 4)
    as double) as avg_nxm_eth_price,
    cast(
      0.01028 + (mcr.mcr_eth_total / 5800000) * power((cp.avg_capital_pool_eth_total / mcr.mcr_eth_total), 4) * cp.avg_eth_usd_price
    as double) as avg_nxm_usd_price
  from capital_pool cp
    inner join mcr on cp.block_date = mcr.block_date
  where cp.block_date < timestamp '2023-11-21'
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as avg_eth_usd_price
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2023-11-21'
  group by 1
),

nxm_daily_internal_price_avgs AS (
  select
    p.block_date,
    p.avg_eth_usd_price,
    avg(cast(ramm.output_internalPrice as double)) / 1e18 as avg_nxm_eth_price
  from daily_avg_eth_prices p
    left join nexusmutual_ethereum.Ramm_call_getInternalPriceAndUpdateTwap ramm on p.block_date = date_trunc('day', ramm.call_block_time)
  group by 1, 2
),

nxm_filled_null_cnts as (
  select
    block_date,
    avg_eth_usd_price,
    avg_nxm_eth_price,
    count(avg_nxm_eth_price) over (order by block_date) as avg_nxm_eth_price_count
  from nxm_daily_internal_price_avgs
),

nxm_daily_price_post_ramm as (
  select
    block_date,
    avg_eth_usd_price,
    first_value(avg_nxm_eth_price) over (partition by avg_nxm_eth_price_count order by block_date) as avg_nxm_eth_price
  from nxm_filled_null_cnts
),

nxm_daily_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from nxm_daily_price_pre_ramm
  union all
  select
    block_date,
    avg_eth_usd_price,
    avg_nxm_eth_price,
    avg_nxm_eth_price * avg_eth_usd_price as avg_nxm_usd_price
  from nxm_daily_price_post_ramm
)

select 
  block_date,
  avg_eth_usd_price,
  coalesce(avg_nxm_eth_price, lag(avg_nxm_eth_price) over (order by block_date)) as avg_nxm_eth_price,
  coalesce(avg_nxm_usd_price, lag(avg_nxm_usd_price) over (order by block_date)) as avg_nxm_usd_price
from nxm_daily_prices
order by 1 desc
