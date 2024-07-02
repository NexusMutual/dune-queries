select
  date_trunc('month', block_date) as block_month,
  sum(cover_sold) as cover_sold,
  sum(eth_active_cover) as eth_active_cover,
  sum(eth_eth_cover) as eth_eth_cover,
  sum(dai_eth_cover) as dai_eth_cover,
  sum(usdc_eth_cover) as usdc_eth_cover,
  sum(usd_active_cover) as usd_active_cover,
  sum(eth_usd_cover) as eth_usd_cover,
  sum(dai_usd_cover) as dai_usd_cover,
  sum(usdc_usd_cover) as usdc_usd_cover,
  sum(eth_premium) as eth_premium,
  sum(eth_eth_premium) as eth_eth_premium,
  sum(dai_eth_premium) as dai_eth_premium,
  sum(nxm_eth_premium) as nxm_eth_premium,
  sum(usd_premium) as usd_premium,
  sum(eth_usd_premium) as eth_usd_premium,
  sum(dai_usd_premium) as dai_usd_premium,
  sum(nxm_usd_premium) as nxm_usd_premium
from query_3889661
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
group by 1
order by 1 desc
