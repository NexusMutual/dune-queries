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
),

active_covers_agg as (
  select
    staking_pool_id,
    staking_pool,
    product_name,
    sum(if(
      '{{display_currency}}' = 'USD',
      eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount,
      eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount
    )) as cover_amount
  from active_covers
  group by 1, 2, 3
)

select
  staking_pool_id,
  staking_pool,
  product_name,
  cover_amount,
  sum(cover_amount) over (partition by staking_pool) as pool_cover_amount
from active_covers_agg
order by 5 desc, 4 desc
