select
  block_date,
  nxm_mint,
  nxm_burn,
  total_nxm_mint,
  total_nxm_burn,
  total_nxm
from query_4514857 -- NXM supply base
where block_date >= cast('{{Start Date}}' as timestamp)
  and block_date <= cast('{{End Date}}' as timestamp)
order by 1 desc
