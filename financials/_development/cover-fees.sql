with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

covers as (
  select
    c.cover_id,
    cover_start_date,
    cover_end_date,
    c.premium_asset,
    c.premium * if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price) as premium_usd,
    c.premium * if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price) / p.avg_eth_usd_price as premium_eth
  --from query_3788367 c -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1 c
    inner join daily_avg_prices p on c.block_date = p.block_date
  union all
  select
    c.cover_id,
    cover_start_date,
    cover_end_date,
    c.premium_asset,
    c.premium_incl_commission * p.avg_nxm_usd_price as premium_usd,
    c.premium_incl_commission * p.avg_nxm_eth_price as premium_eth
  from query_4599092 c -- covers v2 - base root (fallback query)
    inner join daily_avg_prices p on c.block_date = p.block_date
  where c.is_migrated = false
),

premium_aggs as (
  select
    date_trunc('month', cover_start_date) as cover_month,
    sum(premium_usd) as premium_usd,
    sum(premium_eth) as premium_eth,
    sum(premium_usd) filter (where premium_asset = 'ETH') as premium_eth_usd,
    sum(premium_eth) filter (where premium_asset = 'ETH') as premium_eth_eth,
    sum(premium_usd) filter (where premium_asset = 'DAI') as premium_dai_usd,
    sum(premium_eth) filter (where premium_asset = 'DAI') as premium_dai_eth,
    sum(premium_usd) filter (where premium_asset = 'USDC') as premium_usdc_usd,
    sum(premium_eth) filter (where premium_asset = 'USDC') as premium_usdc_eth,
    sum(premium_usd) filter (where premium_asset = 'cbBTC') as premium_cbbtc_usd,
    sum(premium_eth) filter (where premium_asset = 'cbBTC') as premium_cbbtc_eth,
    sum(premium_usd) filter (where premium_asset = 'NXM') as premium_nxm_usd,
    sum(premium_eth) filter (where premium_asset = 'NXM') as premium_nxm_eth
  from covers
  group by 1
)

select
  *
from premium_aggs
order by 1 desc
