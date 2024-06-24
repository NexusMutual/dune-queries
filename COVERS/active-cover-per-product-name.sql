with

active_covers as (
  select
    cover_id,
    product_name,
    --ETH
    eth_cover_amount,
    eth_usd_cover_amount,
    --DAI
    dai_eth_cover_amount,
    dai_usd_cover_amount,
    --USDC
    usdc_eth_cover_amount,
    usdc_usd_cover_amount
  from query_3834200 -- active covers base (fallback) query
)

select
  product_name,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount
  )) as cover_amount
from active_covers
group by 1
order by 2 desc
