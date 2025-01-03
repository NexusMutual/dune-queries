with

swaps_x_bv as (
  select
    block_date,
    nxm_in,
    eth_out,
    nxm_eth_swap_price,
    nxm_usd_swap_price,
    eth_book_value,
    usd_book_value,
    if('{{currency}}' = 'USD', nxm_usd_swap_price, nxm_eth_swap_price) as swap_price,
    if('{{currency}}' = 'USD', usd_book_value, eth_book_value) as book_value
  from query_4516074 -- RAMM swaps below - base
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
