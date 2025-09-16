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
    usdc_usd_cover_amount,
    --cbBTC
    cbbtc_eth_cover_amount,
    cbbtc_usd_cover_amount
  from query_5785377 -- active covers - base root
)

select
  product_name,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount + cbbtc_eth_cover_amount
  )) as cover_amount,
  sum(if(
    '{{display_currency}}' = 'USD', usdc_usd_cover_amount, usdc_eth_cover_amount
  )) as cover_amount_usdc,
  sum(if(
    '{{display_currency}}' = 'USD', cbbtc_usd_cover_amount, cbbtc_eth_cover_amount
  )) as cover_amount_cbbtc,
  sum(if(
    '{{display_currency}}' = 'USD', dai_usd_cover_amount, dai_eth_cover_amount
  )) as cover_amount_dai,
  sum(if(
    '{{display_currency}}' = 'USD', eth_usd_cover_amount, eth_cover_amount
  )) as cover_amount_eth
from active_covers
group by 1
order by 2 desc
