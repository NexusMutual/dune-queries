with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from query_3789851 -- prices base (fallback) query
),

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    syndicate as staking_pool,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured as cover_amount,
    premium_asset,
    premium,
    cover_owner
  --from query_3788367 -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1
  union all
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    staking_pool,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured * partial_cover_amount / sum(partial_cover_amount) over (partition by cover_id) as cover_amount,
    premium_asset,
    premium_incl_commission as premium,
    cover_owner
  --from query_3788370 -- covers v2 base (fallback) query
  from nexusmutual_ethereum.covers_v2
  where is_migrated = false
),

covers_ext as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    date_diff('day', cover_start_date, cover_end_date) as cover_period,
    staking_pool,
    product_type,
    product_name,
    cover_asset,
    if(cover_asset = 'ETH', cover_amount, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', cover_amount, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', cover_amount, 0) as usdc_cover_amount,
    premium_asset,
    if(staking_pool = 'v1' and cover_asset = 'ETH', premium, 0) as eth_premium_amount,
    if(staking_pool = 'v1' and cover_asset = 'DAI', premium, 0) as dai_premium_amount,
    if(staking_pool <> 'v1', premium, 0) as nxm_premium_amount,
    cover_owner
  from covers
),

cover_sales as (
  select
    p.block_date,
    c_start.cover_id,
    c_start.cover_start_date,
    c_start.cover_end_date,
    c_start.cover_period,
    c_start.staking_pool,
    c_start.product_type,
    c_start.product_name,
    c_start.cover_owner,
    --== cover ==
    --ETH
    coalesce(c_start.eth_cover_amount, 0) as eth_eth_cover,
    coalesce(c_start.eth_cover_amount * p.avg_eth_usd_price, 0) as eth_usd_cover,
    --DAI
    coalesce(c_start.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_cover,
    coalesce(c_start.dai_cover_amount * p.avg_dai_usd_price, 0) as dai_usd_cover,
    --USDC
    coalesce(c_start.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_cover,
    coalesce(c_start.usdc_cover_amount * p.avg_usdc_usd_price, 0) as usdc_usd_cover,
    --== fees ==
    --ETH
    coalesce(c_start.eth_premium_amount, 0) as eth_eth_premium,
    coalesce(c_start.eth_premium_amount * p.avg_eth_usd_price, 0) as eth_usd_premium,
    --DAI
    coalesce(c_start.dai_premium_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_premium,
    coalesce(c_start.dai_premium_amount * p.avg_dai_usd_price, 0) as dai_usd_premium,
    --NXM
    coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_premium,
    coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price, 0) as nxm_usd_premium
  from daily_avg_prices p
    inner join covers_ext c_start on ds.block_date = c_start.cover_start_date
)

select
  *
from cover_sales
