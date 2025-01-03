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

internal_price as (
  select
    date_trunc('minute', call_block_time) as block_minute,
    output_internalPrice / 1e18 as internal_price_nxm
  from nexusmutual_ethereum.Ramm_call_getInternalPriceAndUpdateTwap
)

select
  ip.block_minute,
  ip.internal_price_nxm,
  ip.internal_price_nxm * p.avg_eth_usd_price as internal_price_usd
from internal_price ip
  inner join prices p on ip.block_minute = p.block_minute
order by 1 desc
