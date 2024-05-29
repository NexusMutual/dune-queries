with

capital_pool as (
  select * from query_3773633 -- to be replaced with CP spell
  where block_date >= timestamp '2019-11-06'
),

mcr_events as (
  select
    date_trunc('day', evt_block_time) as block_date,
    cast(mcrEtherx100 as double) / 1e18 as mcr_eth
  from nexusmutual_ethereum.MCR_evt_MCREvent
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    cast(mcr as double) / 1e18 as mcr_eth
  from nexusmutual_ethereum.MCR_evt_MCRUpdated
),

mcr as (
  select
    cp.block_date,
    cp.avg_capital_pool_eth_total,
    avg(me.mcr_eth) as mcr_eth
  from capital_pool cp
    left join mcr_events me on cp.block_date = me.block_date
  group by 1, 2
),

mcr_filled_null_cnts as (
  select
    block_date,
    avg_capital_pool_eth_total,
    mcr_eth,
    count(mcr_eth) over (order by block_date) as mcr_eth_count
  from mcr
),

mcr_percent as (
  select
    block_date,
    avg_capital_pool_eth_total,
    mcr_eth,
    avg_capital_pool_eth_total * 100 / first_value(mcr_eth) over (partition by mcr_eth_count order by block_date) as mcr_percentage
  from mcr_filled_null_cnts
)

select *
from mcr_percent
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
order by block_date desc
