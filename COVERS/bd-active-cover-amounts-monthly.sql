select
  date_trunc('month', block_date) as block_month,
  sum(active_cover_sold) as cover_sold,
  sum(eth_active_cover) as eth_active_cover,
  sum(eth_eth_active_cover) as eth_eth_cover,
  sum(dai_eth_active_cover) as dai_eth_cover,
  sum(usdc_eth_active_cover) as usdc_eth_cover,
  sum(usd_active_active_cover) as usd_active_cover,
  sum(eth_usd_active_cover) as eth_usd_cover,
  sum(dai_usd_active_cover) as dai_usd_cover,
  sum(usdc_usd_active_cover) as usdc_usd_cover,
  sum(eth_active_premium) as eth_premium,
  sum(eth_eth_active_premium) as eth_eth_premium,
  sum(dai_eth_active_premium) as dai_eth_premium,
  sum(nxm_eth_active_premium) as nxm_eth_premium,
  sum(usd_active_premium) as usd_premium,
  sum(eth_usd_active_premium) as eth_usd_premium,
  sum(dai_usd_active_premium) as dai_usd_premium,
  sum(nxm_usd_active_premium) as nxm_usd_premium
from query_3889661
where block_date >= now() - interval '2' year
group by 1
order by 1 desc
