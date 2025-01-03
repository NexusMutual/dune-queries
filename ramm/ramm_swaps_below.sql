with

book_value as (
  select
    cp.block_date,
    cp.avg_eth_usd_price,
    cp.avg_capital_pool_usd_total / ns.total_nxm as usd_book_value,
    cp.avg_capital_pool_eth_total / ns.total_nxm as eth_book_value
  --from query_3773633 cp -- Capital Pool base (fallback) query
  from nexusmutual_ethereum.capital_pool_totals cp
    inner join query_4514857 ns -- NXM supply base
      on cp.block_date = ns.block_date
),

swaps_below as (
  select
    date_trunc('day', block_time) as block_date,
    sum(amount_in) as nxm_in,
    sum(amount_out) as eth_out,
    avg(swap_price) as nxm_eth_swap_price
  from query_4498669 -- RAMM swaps - base
  where swap_type = 'below'
  group by 1
),

swaps_x_bv as (
  select
    bv.block_date,
    s.nxm_in,
    s.eth_out,
    s.nxm_eth_swap_price,
    s.nxm_eth_swap_price * bv.avg_eth_usd_price as nxm_usd_swap_price,
    bv.eth_book_value,
    bv.usd_book_value,
    if('{{currency}}' = 'USD', s.nxm_eth_swap_price * bv.avg_eth_usd_price, s.nxm_eth_swap_price) as swap_price,
    if('{{currency}}' = 'USD', bv.usd_book_value, bv.eth_book_value) as book_value
  from book_value bv
    inner join swaps_below s on bv.block_date = s.block_date
)

select
  block_date,
  nxm_in,
  eth_out,
  nxm_eth_swap_price,
  nxm_usd_swap_price,
  eth_book_value,
  usd_book_value,
  swap_price,
  book_value,
  swap_price / book_value as redemption_pct
from swaps_x_bv
order by 1 desc
