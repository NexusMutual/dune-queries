select
  date_trunc('month', block_date) as block_month,
  -- sum of the number of covers sold in any given month:
  sum(cover_sold) as cover_sold,
  -- Monthly Active Cover = average Active Cover Amount:
  avg(eth_active_cover) as avg_eth_active_cover,
  avg(eth_eth_active_cover) as avg_eth_eth_cover,
  avg(dai_eth_active_cover) as avg_dai_eth_cover,
  avg(usdc_eth_active_cover) as avg_usdc_eth_cover,
  avg(usd_active_cover) as avg_usd_active_cover,
  avg(eth_usd_active_cover) as avg_eth_usd_cover,
  avg(dai_usd_active_cover) as avg_dai_usd_cover,
  avg(usdc_usd_active_cover) as avg_usdc_usd_cover,
  -- Monthly Cover Amount = sum of the Cover Amount for all covers sold in any given month
  sum(eth_cover) as eth_cover,
  sum(eth_eth_cover) as eth_eth_cover,
  sum(dai_eth_cover) as dai_eth_cover,
  sum(usdc_eth_cover) as usdc_eth_cover,
  sum(usd_cover) as usd_cover,
  sum(eth_usd_cover) as eth_usd_cover,
  sum(dai_usd_cover) as dai_usd_cover,
  sum(usdc_usd_cover) as usdc_usd_cover,
  -- Monthly Premium = sum of the premiums paid in any given month
  sum(eth_premium) as eth_premium,
  sum(eth_eth_premium) as eth_eth_premium,
  sum(dai_eth_premium) as dai_eth_premium,
  sum(usdc_eth_premium) as usdc_eth_premium,
  sum(nxm_eth_premium) as nxm_eth_premium,
  sum(usd_premium) as usd_premium,
  sum(eth_usd_premium) as eth_usd_premium,
  sum(dai_usd_premium) as dai_usd_premium,
  sum(usdc_usd_premium) as usdc_usd_premium,
  sum(nxm_usd_premium) as nxm_usd_premium
from query_4200150 -- OC cover base
where block_date >= now() - interval '3' year
group by 1
order by 1 desc
