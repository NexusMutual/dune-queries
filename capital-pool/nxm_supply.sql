select
  block_date,
  nxm_mint,
  nxm_burn,
  total_nxm_mint,
  total_nxm_burn,
  total_nxm
from query_4514857 -- NXM supply base
order by 1 desc
