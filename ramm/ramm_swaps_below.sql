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
    date_trunc('minute', block_time) as block_minute,
    member,
    amount_in as nxm_in,
    amount_out as eth_out,
    swap_price as nxm_eth_swap_price
  from query_4498669 -- RAMM swaps - base
  where swap_type = 'below'
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
