with

mcr as (
  select
    block_date,
    avg_eth_usd_price,
    mcr_eth_total,
    mcr_floor_total,
    mcr_cover_min_total,
    if('{{display_currency}}' = 'USD', avg_eth_usd_price, 1.0) as price_display_curr
  from query_3787908 -- MCR base (fallback) query
)

select
  block_date as date,
  mcr_eth_total * price_display_curr as mcr_eth_display_curr,
  mcr_floor_total * price_display_curr as mcr_floor_display_curr,
  mcr_cover_min_total * price_display_curr as mcr_cover_min_display_curr
from mcr
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
order by 1 desc
