with

eth_injected as (
  select
    date_trunc('day', evt_block_time) as block_date,
    sum(value) / 1e18 as eth_injected
  from nexusmutual_ethereum.Ramm_evt_EthInjected
  group by 1
),

cumulative_injected as (
  select
    block_date,
    eth_injected,
    sum(eth_injected) over (order by block_date) as cummulative_injected
  from eth_injected
)

select
  block_date,
  eth_injected,
  cummulative_injected,
  cast(43850.0 as double) - cummulative_injected as remaining_budget
from cumulative_injected
order by 1 desc
