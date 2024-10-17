with

capital_pool as (
  select *
  from query_3773633 -- Capital Pool base (fallback) query
  --from nexusmutual_ethereum.capital_pool_totals
),

display_currency_total as (
  select
    block_date,
    if('{{display_currency}}' = 'USD', avg_capital_pool_usd_total, avg_capital_pool_eth_total) as capital_pool_display_curr
  from capital_pool
),

nxm_events as (
  select
    block_date,
    sum(nxm_amount) as nxm_amount
  from (
    select
      date_trunc('day', evt_block_time) as block_date,
      sum(cast(value as double) / 1e18) as nxm_amount
    from nexusmutual_ethereum.NXMToken_evt_Transfer
    where "from" = 0x0000000000000000000000000000000000000000
    group by 1
    union all
    select
      date_trunc('day', evt_block_time) as block_date,
      -1 * sum(cast(value as double) / 1e18) as nxm_amount
    from nexusmutual_ethereum.NXMToken_evt_Transfer
    where "to" = 0x0000000000000000000000000000000000000000
    group by 1
  ) t
  group by 1
),

nxm_supply as (
  select
    block_date,
    sum(nxm_amount) over (order by block_date) as nxm_total
  from nxm_events
)

select
  ct.block_date,
  ct.capital_pool_display_curr,
  ns.nxm_total,
  ct.capital_pool_display_curr / ns.nxm_total as book_value
from display_currency_total ct
  inner join nxm_supply ns on ct.block_date = ns.block_date
where ct.block_date >= timestamp '{{Start Date}}'
  and ct.block_date < timestamp '{{End Date}}'
order by block_date desc nulls first
