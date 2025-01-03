select sum((1 - (eth_out / nxm_in / eth_book_value)) * nxm_in) / sum(nxm_in) * 100 as avg_discount_bv_pct
from query_4516074 -- RAMM swaps below - base
