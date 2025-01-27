with

capital_pool as (
  select
    block_date,
    if('{{display_currency}}' = 'USD', avg_capital_pool_usd_total, avg_capital_pool_eth_total) as capital_pool_display_curr
  from query_4627588 -- Capital Pool - base root
)

select
  cp.block_date,
  cp.capital_pool_display_curr,
  ns.total_nxm as nxm_total,
  cp.capital_pool_display_curr / ns.total_nxm as book_value
from capital_pool cp
  inner join query_4514857 ns -- NXM supply base
    on cp.block_date = ns.block_date
where cp.block_date >= timestamp '{{Start Date}}'
  and cp.block_date < timestamp '{{End Date}}'
order by 1 desc
