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
)

select
  date_trunc('month', v.block_date) as block_month,
  sum(v.nxm_in) as nxm_in,
  sum(v.eth_out) as eth_eth_out,
  sum(v.eth_out * p.avg_eth_usd_price) as eth_usd_out,
  sum(v.eth_in) as eth_eth_in,
  sum(v.eth_in * p.avg_eth_usd_price) as eth_usd_in,
  sum(v.nxm_out) as nxm_out
from volume_transacted v
  inner join daily_avg_prices p on v.block_date = p.block_date
group by 1
order by 1 desc
