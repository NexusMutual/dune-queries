with

cummulative_volume_transacted as (
  select
    block_time,
    sum(case when token_in = 'ETH' then amount_in end) over (order by block_time) as cummulative_eth_in,
    sum(case when token_in = 'NXM' then amount_in end) over (order by block_time) as cummulative_nxm_in,
    sum(case when token_out = 'ETH' then amount_out end) over (order by block_time) as cummulative_eth_out,
    sum(case when token_out = 'NXM' then amount_out end) over (order by block_time) as cummulative_nxm_out
  from query_4498669 -- RAMM swaps - base
)

select distinct
  block_time,
  cummulative_eth_in,
  cummulative_eth_out,
  cummulative_nxm_out,
  cummulative_nxm_in
from cummulative_volume_transacted
where block_time >= cast('{{Start Date}}' as timestamp)
  and block_time <= cast('{{End Date}}' as timestamp)
order by 1 desc
