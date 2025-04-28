with


items (id, item_1, item_2) as (
  values
    (1, '🐢 latest cover', null),
    (2, 'cover id', null),
    (3, 'listing', null),
    (4, 'native cover', null),
    (5, 'cover amount', null),
    (6, 'native premium', null),
    (7, 'premium amount', null),
    (8, '--------------------------------', '--------------------------------'),
    (9, '🐢 active cover (Ξ)', '🐢 active cover ($)'),
    (10, 'active cover (Ξ)', 'active cover ($)'),
    (11, 'median active cover (Ξ)', 'median active cover ($)'),
    (12, 'mean active cover (Ξ)', 'mean active cover ($)'),
    (13, '--------------------------------', '--------------------------------'),
    (14, '🐢 active premium (Ξ)', '🐢 active premium ($)'),
    (15, 'active premium (Ξ)', 'active premium ($)'),
    (16, 'median active premium (Ξ)', 'median active premium ($)'),
    (17, 'mean active premium (Ξ)', 'mean active premium ($)')
),

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
),

combined_stats as (
  select
    -- latest cover
    cast(c.cover_id as varchar) as cover_id,
    c.listing,
    c.native_cover,
    c.cover_amount,
    c.native_premium,
    c.premium_amount,
    -- active cover stats (ETH)
    'Ξ' || format('%,.2f', cast(ac.eth_active_cover as double)) as eth_active_cover,
    'Ξ' || format('%,.2f', cast(ac.median_eth_active_cover as double)) as median_eth_active_cover,
    'Ξ' || format('%,.2f', cast(ac.mean_eth_active_cover as double)) as mean_eth_active_cover,
    -- active cover stats (USD)
    '$' || format('%,.2f', cast(ac.usd_active_cover as double)) as usd_active_cover,
    '$' || format('%,.2f', cast(ac.median_usd_active_cover as double)) as median_usd_active_cover,
    '$' || format('%,.2f', cast(ac.mean_usd_active_cover as double)) as mean_usd_active_cover,
    -- active premium stats (ETH)
    'Ξ' || format('%,.2f', cast(ac.eth_active_premium as double)) as eth_active_premium,
    'Ξ' || format('%,.2f', cast(ac.median_eth_active_premium as double)) as median_eth_active_premium,
    'Ξ' || format('%,.2f', cast(ac.mean_eth_active_premium as double)) as mean_eth_active_premium,
    -- active premium stats (USD)
    '$' || format('%,.2f', cast(ac.usd_active_premium as double)) as usd_active_premium,
    '$' || format('%,.2f', cast(ac.median_usd_active_premium as double)) as median_usd_active_premium,
    '$' || format('%,.2f', cast(ac.mean_usd_active_premium as double)) as mean_usd_active_premium
  from latest_cover c
    cross join latest_active_cover ac
)

select
  i.item_1,
  case i.item_1
    when 'cover id' then cs.cover_id
    when 'listing' then cs.listing
    when 'native cover' then cs.native_cover
    when 'cover amount' then cs.cover_amount
    when 'native premium' then cs.native_premium
    when 'premium amount' then cs.premium_amount
    when 'active cover (Ξ)' then cs.eth_active_cover
    when 'median active cover (Ξ)' then cs.median_eth_active_cover
    when 'mean active cover (Ξ)' then cs.mean_eth_active_cover
    when 'active premium (Ξ)' then cs.eth_active_premium
    when 'median active premium (Ξ)' then cs.median_eth_active_premium
    when 'mean active premium (Ξ)' then cs.mean_eth_active_premium
  end as value_1,
  i.item_2,
  case i.item_2
    when 'active cover ($)' then cs.usd_active_cover
    when 'median active cover ($)' then cs.median_usd_active_cover
    when 'mean active cover ($)' then cs.mean_usd_active_cover
    when 'active premium ($)' then cs.usd_active_premium
    when 'median active premium ($)' then cs.median_usd_active_premium
    when 'mean active premium ($)' then cs.mean_usd_active_premium
  end as value_2
from items i
  cross join combined_stats cs
order by i.id
