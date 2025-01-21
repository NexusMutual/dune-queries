with

covers as (
  select
    block_time,
    block_number,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    if(cover_end_time < now(), cover_end_date, current_date) as cover_end_date_join,
    staking_pool,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    premium_asset,
    premium,
    premium_incl_commission as premium_nxm,
    partial_cover_amount,
    sum(partial_cover_amount) over (partition by cover_id) as total_cover_amount,
    cover_owner,
    tx_hash
  from query_4599092 -- covers v2 - base ref (fallback query)
),

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_cbbtc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
)

select
  c.cover_id,
  if(c.cover_end_time >= now(), 'Active', 'Expired') as cover_status,
  c.cover_asset,
  c.sum_assured as native_cover_amount,
  case
    when c.cover_asset = 'ETH' then c.sum_assured * p_start.avg_eth_usd_price
    when c.cover_asset = 'DAI' then c.sum_assured * p_start.avg_dai_usd_price
    when c.cover_asset = 'USDC' then c.sum_assured * p_start.avg_usdc_usd_price
    when c.cover_asset = 'cbBTC' then c.sum_assured * p_start.avg_cbbtc_usd_price
  end as cover_start_usd,
  case
    when c.cover_asset = 'ETH' then c.sum_assured
    when c.cover_asset = 'DAI' then (c.sum_assured * p_start.avg_dai_usd_price) / p_start.avg_eth_usd_price
    when c.cover_asset = 'USDC' then (c.sum_assured * p_start.avg_usdc_usd_price) / p_start.avg_eth_usd_price
    when c.cover_asset = 'cbBTC' then (c.sum_assured * p_start.avg_cbbtc_usd_price) / p_start.avg_eth_usd_price
  end as cover_start_eth,
  case
    when c.cover_asset = 'ETH' then c.sum_assured * p_end.avg_eth_usd_price
    when c.cover_asset = 'DAI' then c.sum_assured * p_end.avg_dai_usd_price
    when c.cover_asset = 'USDC' then c.sum_assured * p_end.avg_usdc_usd_price
    when c.cover_asset = 'cbBTC' then c.sum_assured * p_end.avg_cbbtc_usd_price
  end as cover_end_usd,
  case
    when c.cover_asset = 'ETH' then c.sum_assured
    when c.cover_asset = 'DAI' then (c.sum_assured * p_end.avg_dai_usd_price) / p_end.avg_eth_usd_price
    when c.cover_asset = 'USDC' then (c.sum_assured * p_end.avg_usdc_usd_price) / p_end.avg_eth_usd_price
    when c.cover_asset = 'cbBTC' then (c.sum_assured * p_end.avg_cbbtc_usd_price) / p_end.avg_eth_usd_price
  end as cover_end_eth,
  c.staking_pool,
  c.sum_assured * c.partial_cover_amount / c.total_cover_amount as staking_pool_cover_amount,
  c.premium_asset,
  c.premium_nxm,
  case
    when c.cover_asset = 'ETH' then c.premium_nxm * p_start.avg_nxm_eth_price
    when c.cover_asset = 'DAI' then (c.premium_nxm * p_start.avg_nxm_usd_price) / p_start.avg_dai_usd_price
    when c.cover_asset = 'USDC' then (c.premium_nxm * p_start.avg_nxm_usd_price) / p_start.avg_usdc_usd_price
    when c.cover_asset = 'cbBTC' then (c.premium_nxm * p_start.avg_nxm_usd_price) / p_start.avg_cbbtc_usd_price
  end as premium_native,
  c.premium_nxm * p_start.avg_nxm_usd_price as premium_usd,
  coalesce(c.product_type, 'unknown') as product_type,
  coalesce(c.product_name, 'unknown') as product_name,
  c.cover_start_time,
  c.cover_end_time,
  c.cover_owner,
  date_diff('day', c.cover_start_time, c.cover_end_time) as cover_period,
  c.tx_hash
from covers c
  left join daily_avg_prices p_start on c.cover_start_date = p_start.block_date
  left join daily_avg_prices p_end on c.cover_end_date_join = p_end.block_date
order by c.cover_id desc
