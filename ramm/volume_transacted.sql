with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

volume_transacted as (
  select
    date_trunc('day', block_time) as block_date,
    sum(case when token_in = 'ETH' then amount_in end) as eth_in,
    sum(case when token_in = 'NXM' then amount_in end) as nxm_in,
    sum(case when token_out = 'ETH' then amount_out end) as eth_out,
    sum(case when token_out = 'NXM' then amount_out end) as nxm_out
  from query_4498669 -- RAMM swaps - base
  group by 1
),

cumulative_volume_transacted as (
  select
    v.block_date,
    v.eth_in as eth_eth_in,
    v.eth_in * p.avg_eth_usd_price as eth_usd_in,
    v.nxm_in,
    v.eth_out as eth_eth_out,
    v.eth_out * p.avg_eth_usd_price as eth_usd_out,
    v.nxm_out,
    sum(v.eth_in) over (order by v.block_date) as cummulative_eth_eth_in,
    sum(v.eth_in * p.avg_eth_usd_price) over (order by v.block_date) as cummulative_eth_usd_in,
    sum(v.nxm_in) over (order by v.block_date) as cummulative_nxm_in,
    sum(v.eth_out) over (order by v.block_date) as cummulative_eth_eth_out,
    sum(v.eth_out * p.avg_eth_usd_price) over (order by v.block_date) as cummulative_eth_usd_out,
    sum(v.nxm_out) over (order by v.block_date) as cummulative_nxm_out
  from volume_transacted v
    inner join daily_avg_prices p on v.block_date = p.block_date
)

select
  block_date,
  eth_eth_in,
  eth_usd_in,
  if('{{currency}}' = 'USD', eth_usd_in, eth_eth_in) as eth_in,
  nxm_in,
  eth_eth_out,
  eth_usd_out,
  if('{{currency}}' = 'USD', eth_usd_out, eth_eth_out) as eth_out,
  nxm_out,
  cummulative_eth_eth_in,
  cummulative_eth_usd_in,
  if('{{currency}}' = 'USD', cummulative_eth_usd_in, cummulative_eth_eth_in) as cummulative_eth_in,
  cummulative_nxm_in,
  cummulative_eth_eth_out,
  cummulative_eth_usd_out,
  if('{{currency}}' = 'USD', cummulative_eth_usd_out, cummulative_eth_eth_out) as cummulative_eth_out,
  cummulative_nxm_out
from cumulative_volume_transacted
order by 1 desc
