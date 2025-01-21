with

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
    c.premium_incl_commission * p.avg_nxm_usd_price / p.avg_eth_usd_price as premium_eth
  from query_4599092 c -- covers v2 - base ref (fallback query)
    inner join daily_avg_prices p on c.block_date = p.block_date
  where c.is_migrated = false
),

premium_aggs as (
  select
    year(cover_start_date) as year,
    quarter(cover_start_date) as quarter,
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
  where cover_start_date >= timestamp '{{Start Date}}'
    and cover_start_date < timestamp '{{End Date}}'
  group by 1, 2
)

select
  year,
  quarter,
  concat('Q', cast(quarter as varchar)) as quarter_label,
  if('{{display_currency}}' = 'USD', premium_usd, premium_eth) as quarterly_premium,
  if('{{display_currency}}' = 'USD', sum(premium_usd) over (partition by year), sum(premium_eth) over (partition by year)) as annual_premium,
  if('{{display_currency}}' = 'USD', sum(premium_usd) over (), sum(premium_eth) over ()) as total_premium,
  if('{{display_currency}}' = 'USD', sum(premium_eth_usd) over (), sum(premium_eth_eth) over ()) as eth_premium,
  if('{{display_currency}}' = 'USD', sum(premium_dai_usd) over (), sum(premium_dai_eth) over ()) as dai_premium,
  if('{{display_currency}}' = 'USD', sum(premium_usdc_usd) over (), sum(premium_usdc_eth) over ()) as usdc_premium,
  if('{{display_currency}}' = 'USD', sum(premium_cbbtc_usd) over (), sum(premium_cbbtc_eth) over ()) as cbbtc_premium,
  if('{{display_currency}}' = 'USD', sum(premium_nxm_usd) over (), sum(premium_nxm_eth) over ()) as nxm_premium
from premium_aggs
order by 1, 2
