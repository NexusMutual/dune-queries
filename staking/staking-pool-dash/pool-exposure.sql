with

params as (
  select cast(split_part('{{pool}}', ' : ', 1) as int) as pool_id
),

active_covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    staking_pool_id,
    product_id,
    product_type,
    product_name,
    cover_asset,
    --ETH
    eth_cover_amount,
    eth_usd_cover_amount,
    --DAI
    dai_cover_amount,
    dai_eth_cover_amount,
    dai_usd_cover_amount,
    --USDC
    usdc_cover_amount,
    usdc_eth_cover_amount,
    usdc_usd_cover_amount,
    --cbBTC
    cbbtc_cover_amount,
    cbbtc_eth_cover_amount,
    cbbtc_usd_cover_amount
  from query_3834200 -- active covers base (fallback) query
  --from nexusmutual_ethereum.active_covers
  where cast(staking_pool_id as int) in (select pool_id from params)
)

select
  --coalesce(product_name, '**Totals**') as listing,
  product_name as listing,
  sum(usdc_cover_amount) as usdc_cover_amount,
  sum(cbbtc_cover_amount) as cbbtc_cover_amount,
  sum(dai_cover_amount) as dai_cover_amount,
  sum(eth_cover_amount) as eth_cover_amount,
  sum(usdc_usd_cover_amount + cbbtc_usd_cover_amount + dai_usd_cover_amount + eth_usd_cover_amount) as total_usd_cover_amount,
  sum(usdc_eth_cover_amount + cbbtc_eth_cover_amount + dai_eth_cover_amount + eth_cover_amount) as total_eth_cover_amount
from active_covers
group by 1
order by 1

/*
group by grouping sets (
  (product_name), -- individual product totals
  ()              -- grand total for all products
)
order by case when product_name is null then 1 else 0 end, product_name
*/
