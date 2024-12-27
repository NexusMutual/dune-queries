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

swaps_below as (
  select
    date_trunc('minute', evt_block_time) as block_minute,
    member,
    nxmIn / 1e18 as nxm_in,
    ethOut / 1e18 as eth_out,
    cast(ethOut as double) / cast(nxmIn as double) as nxm_eth_swap_price
  from nexusmutual_ethereum.Ramm_evt_NxmSwappedForEth
)

select
  s.block_minute,
  s.member,
  s.nxm_in,
  s.eth_out,
  s.nxm_eth_swap_price,
  s.nxm_eth_swap_price * p.avg_eth_usd_price as nxm_usd_swap_price
from swaps_below s
  inner join prices p on s.block_minute = p.block_minute     
where s.block_minute >= cast('{{Start Date}}' as timestamp)
  and s.block_minute <= cast('{{End Date}}' as timestamp)
order by 1 desc
