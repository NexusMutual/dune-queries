with

book_value as (
  select
    cp.block_date,
    cp.avg_eth_usd_price,
    cp.avg_capital_pool_usd_total / ns.total_nxm as usd_book_value,
    cp.avg_capital_pool_eth_total / ns.total_nxm as eth_book_value
  from query_4627588 cp -- Capital Pool - base root
    inner join query_4514857 ns -- NXM supply base
      on cp.block_date = ns.block_date
),

swaps_below as (
  select
    date_trunc('day', block_time) as block_date,
    sum(amount_in) as nxm_in_below,
    sum(amount_out) as eth_out_below,
    avg(swap_price) as nxm_eth_swap_price_below
  from query_4498669 -- RAMM swap events - base
  where swap_type = 'below'
  group by 1
),

swaps_above as (
  select
    date_trunc('day', block_time) as block_date,
    sum(amount_out) as nxm_out_above,
    sum(amount_in) as eth_in_above,
    avg(swap_price) as nxm_eth_swap_price_above
  from query_4498669 -- RAMM swap events - base
  where swap_type = 'above'
  group by 1
)

select
  bv.block_date,
  sb.nxm_in_below,
  sb.eth_out_below,
  sb.nxm_eth_swap_price_below,
  sb.nxm_eth_swap_price_below * bv.avg_eth_usd_price as nxm_usd_swap_price_below,
  sa.nxm_out_above,
  sa.eth_in_above,
  sa.nxm_eth_swap_price_above,
  sa.nxm_eth_swap_price_above * bv.avg_eth_usd_price as nxm_usd_swap_price_above,
  bv.eth_book_value,
  bv.usd_book_value,
  bv.avg_eth_usd_price
from book_value bv
  left join swaps_below sb on bv.block_date = sb.block_date
  left join swaps_above sa on bv.block_date = sa.block_date
where bv.block_date >= cast('2023-11-28' as date) -- RAMM go live date
