with

swaps_x_bv as (
  select
    block_date,
    nxm_eth_swap_price_below,
    nxm_usd_swap_price_below,
    nxm_eth_swap_price_above,
    nxm_usd_swap_price_above,
    eth_book_value,
    usd_book_value,
    if('{{currency}}' = 'usd', nxm_usd_swap_price_below, nxm_eth_swap_price_below) as swap_price_below,
    if('{{currency}}' = 'usd', nxm_usd_swap_price_above, nxm_eth_swap_price_above) as swap_price_above,
    if('{{currency}}' = 'usd', usd_book_value, eth_book_value) as book_value
  from query_4516074 -- RAMM swaps - base
)

select
  block_date,
  swap_price_below,
  swap_price_above,
  book_value,
  swap_price_below / book_value as redemption_pct_below,
  1.00 - (swap_price_below / book_value) as discount_below,
  swap_price_above / book_value as redemption_pct_above,
  1.00 - (swap_price_above / book_value) as discount_above
from swaps_x_bv
order by 1 desc
