with

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as eth_price_usd
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2019-11-06'
  group by 1
),

mcr_events as (
  select
    date_trunc('day', evt_block_time) as block_date,
    cast(mcrEtherx100 as double) / 1e18 as mcr_eth,
    cast(7000 as double) as mcr_floor,
    cast(0 as double) as mcr_cover_min
  from nexusmutual_ethereum.mcr_evt_mcrevent
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    cast(mcr as double) / 1e18 as mcr_eth,
    cast(mcrFloor as double) / 1e18 as mcr_floor,
    cast(mcrETHWithGear as double) / 1e18 as mcr_cover_min
  from nexusmutual_ethereum.MCR_evt_MCRUpdated
),

mcr as (
  select
    p.block_date,
    p.eth_price_usd,
    avg(me.mcr_eth) as mcr_eth,
    avg(me.mcr_floor) as mcr_floor,
    avg(me.mcr_cover_min) as mcr_cover_min
  from daily_avg_eth_prices p
    left join mcr_events me on p.block_date = me.block_date
  group by 1, 2
),

mcr_filled_null_cnts as (
  select
    block_date,
    eth_price_usd,
    if('{{display_currency}}' = 'USD', eth_price_usd, 1.0) as price_display_curr,
    mcr_eth,
    mcr_floor,
    mcr_cover_min,
    count(mcr_eth) over (order by block_date) as mcr_eth_count,
    count(mcr_floor) over (order by block_date) as mcr_floor_count,
    count(mcr_cover_min) over (order by block_date) as mcr_cover_min_count
  from mcr
),

mcr_currency as (
  select
    block_date,
    first_value(mcr_eth) over (partition by mcr_eth_count order by block_date) * price_display_curr as mcr_eth_display_curr,
    first_value(mcr_floor) over (partition by mcr_floor_count order by block_date) * price_display_curr as mcr_floor_display_curr,
    first_value(mcr_cover_min) over (partition by mcr_cover_min_count order by block_date) * price_display_curr as mcr_cover_min_display_curr
  from mcr_filled_null_cnts
)

select *
from mcr_currency
order by block_date desc
