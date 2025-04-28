with

covers as (
  select distinct
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_status,
    product_type,
    product_name,
    cover_asset,
    native_cover_amount,
    cover_start_usd as usd_cover_amount,
    cover_start_eth as eth_cover_amount,
    premium_asset,
    sum(premium_native) over (partition by cover_id) as native_premium,
    sum(premium_nxm) over (partition by cover_id) as nxm_premium,
    sum(premium_usd) over (partition by cover_id) as usd_premium,
    cover_owner,
    cover_period
  from query_3810247 -- full list covers v2
),

ordered_covers as (
  select
    *,
    row_number() over (order by cover_id desc) as rn
  from covers
),

latest_cover as (
  select
    cover_id,
    product_name as listing,
    cover_asset || ': ' || format('%,.2f', cast(native_cover_amount as double)) as native_cover,
    '$' || format('%,.2f', cast(usd_cover_amount as double)) || ' | ' || 'Ξ' || format('%,.2f', cast(eth_cover_amount as double)) as cover_amount,
    premium_asset || ': ' || format('%,.2f', cast(native_premium as double)) as native_premium,
    '$' || format('%,.2f', cast(usd_premium as double)) || ' | ' || 'Ξ' || format('%,.2f', cast(nxm_premium as double)) as premium_amount,
    cover_owner,
    cover_period
  from ordered_covers
  where rn = 1
),

active_cover as (
  select
    block_date,
    active_cover,
    eth_active_cover,
    eth_eth_active_cover,
    dai_eth_active_cover,
    usdc_eth_active_cover,
    cbbtc_eth_active_cover,
    eth_active_cover / active_cover as mean_eth_active_cover,
    median_eth_active_cover,
    usd_active_cover,
    eth_usd_active_cover,
    dai_usd_active_cover,
    usdc_usd_active_cover,
    cbbtc_usd_active_cover,
    usd_active_cover / active_cover as mean_usd_active_cover,
    median_usd_active_cover,
    eth_active_premium,
    eth_eth_active_premium,
    dai_eth_active_premium,
    usdc_eth_active_premium,
    cbbtc_eth_active_premium,
    nxm_eth_active_premium,
    eth_active_premium / active_cover as mean_eth_active_premium,
    median_eth_active_premium,
    usd_active_premium,
    eth_usd_active_premium,
    dai_usd_active_premium,
    usdc_usd_active_premium,
    cbbtc_usd_active_premium,
    nxm_usd_active_premium,
    usd_active_premium / active_cover as mean_usd_active_premium,
    median_usd_active_premium,
    row_number() over (order by block_date desc) as rn
  --from query_3889661 -- BD active cover base
  from nexusmutual_ethereum.covers_daily_agg
  where block_date >= now() - interval '7' day
),

latest_active_cover as (
  select
    eth_active_cover,
    median_eth_active_cover,
    mean_eth_active_cover,
    usd_active_cover,
    median_usd_active_cover,
    mean_usd_active_cover,
    eth_active_premium,
    median_eth_active_premium,
    mean_eth_active_premium,
    usd_active_premium,
    median_usd_active_premium,
    mean_usd_active_premium
  from active_cover
  where rn = 1
)

select
  c.cover_id,
  c.listing,
  c.native_cover,
  c.cover_amount,
  c.native_premium,
  c.premium_amount,
  ac.eth_active_cover,
  ac.median_eth_active_cover,
  ac.mean_eth_active_cover,
  ac.usd_active_cover,
  ac.median_usd_active_cover,
  ac.mean_usd_active_cover,
  ac.eth_active_premium,
  ac.median_eth_active_premium,
  ac.mean_eth_active_premium,
  ac.usd_active_premium,
  ac.median_usd_active_premium,
  ac.mean_usd_active_premium
from latest_cover c
  cross join latest_active_cover ac
