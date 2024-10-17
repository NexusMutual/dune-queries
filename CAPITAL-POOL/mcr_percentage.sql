with

capital_pool as (
  select *
  from query_3773633 -- Capital Pool base (fallback) query
  --from nexusmutual_ethereum.capital_pool_totals
  where block_date >= timestamp '2019-11-06'
),

mcr as (
  select
    block_date,
    mcr_eth_total
  --from query_3787908 -- MCR base (fallback) query
  from nexusmutual_ethereum.capital_pool_mcr
),

mcr_percent as (
  select
    cp.block_date,
    cp.avg_capital_pool_eth_total,
    mcr.mcr_eth_total,
    cp.avg_capital_pool_eth_total * 100 / mcr.mcr_eth_total as mcr_percentage
  from capital_pool cp
    inner join mcr on cp.block_date = mcr.block_date
)

select
  block_date,
  avg_capital_pool_eth_total,
  mcr_eth_total,
  mcr_percentage
from mcr_percent
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
order by 1 desc
