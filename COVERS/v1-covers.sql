with

covers as (
  select
    block_time,
    block_date,
    block_number,
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    product_contract,
    syndicate,
    product_name,
    product_type,
    sum_assured,
    premium_asset,
    premium,
    cover_asset,
    premium_nxm,
    cover_owner,
    evt_index,
    tx_hash
  from query_3788367 -- covers v1 base (fallback) query
),

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from query_3789851 -- NXM prices base (fallback) query
)

select
  c.cover_id,
  if(c.cover_end_time >= now(), 'Active', 'Expired') as cover_status,
  c.cover_asset,
  c.sum_assured,
  case
    when c.cover_asset = 'ETH' then c.sum_assured * p_start.avg_eth_usd_price
    when c.cover_asset = 'DAI' then c.sum_assured * p_start.avg_dai_usd_price
  end as cover_start_usd,
  case
    when c.cover_asset = 'ETH' then c.sum_assured
    when c.cover_asset = 'DAI' then (c.sum_assured * p_start.avg_dai_usd_price) / p_start.avg_eth_usd_price
  end as cover_start_eth,
  case
    when c.cover_asset = 'ETH' then c.sum_assured * p_end.avg_eth_usd_price
    when c.cover_asset = 'DAI' then c.sum_assured * p_end.avg_dai_usd_price
  end as cover_end_usd,
  case
    when c.cover_asset = 'ETH' then c.sum_assured
    when c.cover_asset = 'DAI' then (c.sum_assured * p_end.avg_dai_usd_price) / p_end.avg_eth_usd_price
  end as cover_end_eth,
  c.premium_asset,
  c.premium_nxm,
  c.premium,
  case
    when c.cover_asset = 'ETH' then c.premium * p_start.avg_eth_usd_price
    when c.cover_asset = 'DAI' then c.premium * p_start.avg_dai_usd_price
  end as premium_usd,
  c.syndicate,
  c.product_type,
  c.product_name,
  c.cover_start_time,
  c.cover_end_time,
  c.cover_owner,
  date_diff('day', c.cover_start_time, c.cover_end_time) as cover_period,
  c.tx_hash
from covers c
  left join daily_avg_prices p_start on c.cover_start_date = p_start.block_date
  left join daily_avg_prices p_end on c.cover_end_date = p_end.block_date
order by cover_id desc
