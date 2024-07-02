select
  block_date,
  cover_sold,
  eth_active_cover,
  eth_eth_cover,
  dai_eth_cover,
  usdc_eth_cover,
  usd_active_cover,
  eth_usd_cover,
  dai_usd_cover,
  usdc_usd_cover,
  eth_premium,
  eth_eth_premium,
  dai_eth_premium,
  nxm_eth_premium,
  usd_premium,
  eth_usd_premium,
  dai_usd_premium,
  nxm_usd_premium
from query_3889661
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
order by 1 desc
