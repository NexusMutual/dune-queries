with

active_covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    product_type,
    product_name,
    staking_pool_id,
    staking_pool,
    cover_asset,
    sum_assured,
    partial_cover_amount,
    total_cover_amount,
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
  staking_pool,
  product_type,
  product_name,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount
  )) as cover_amount
from active_covers
group by 1, 2, 3
order by 4 desc
