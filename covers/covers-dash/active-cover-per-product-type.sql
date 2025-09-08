with

active_covers as (
  select
    cover_id,
    product_type,
    --ETH
    eth_cover_amount,
    eth_usd_cover_amount,
    --DAI
    dai_eth_cover_amount,
    dai_usd_cover_amount,
    --USDC
    usdc_eth_cover_amount,
    usdc_usd_cover_amount,
    --cbBTC
    cbbtc_eth_cover_amount,
    cbbtc_usd_cover_amount
  from query_3834200 -- active covers base (fallback) query
  --from nexusmutual_ethereum.active_covers
)

select
  product_type,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount + cbbtc_eth_cover_amount
  )) as cover_amount
from active_covers
group by 1
order by 2 desc
