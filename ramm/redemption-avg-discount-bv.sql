select sum((1 - (eth_out_below / nxm_in_below / eth_book_value)) * nxm_in_below) / sum(nxm_in_below) * 100 as avg_discount_bv_pct
from query_4516074 -- RAMM swaps - base
