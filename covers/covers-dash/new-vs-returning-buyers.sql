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
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured,
    cover_owner,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    cast(0 as double) as usdc_cover_amount,
    cast(0 as double) as cbbtc_cover_amount
  --from query_3788367 -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1
  union all
  select distinct
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured,
    cover_owner,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', sum_assured, 0) as cbbtc_cover_amount
  --from query_3788370 -- covers v2 base (fallback) query
  from nexusmutual_ethereum.covers_v2
  where is_migrated = false
),

cover_base as (
  select
    c.cover_owner,
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    --ETH
    c.eth_cover_amount,
    c.eth_cover_amount * p.avg_eth_usd_price as eth_usd_cover_amount,
    --DAI
    c.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_cover_amount,
    c.dai_cover_amount * p.avg_dai_usd_price as dai_usd_cover_amount,
    --USDC
    c.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price as usdc_eth_cover_amount,
    c.usdc_cover_amount * p.avg_usdc_usd_price as usdc_usd_cover_amount,
    --cbBTC
    c.cbbtc_cover_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price as cbbtc_eth_cover_amount,
    c.cbbtc_cover_amount * p.avg_cbbtc_usd_price as cbbtc_usd_cover_amount
  from covers c
    inner join daily_avg_prices p on c.cover_start_date = p.block_date
),

cover_totals as (
  select
    cover_owner,
    cover_id,
    cover_start_date,
    cover_end_date,
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount as usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount + cbbtc_eth_cover_amount as eth_cover_amount,
    row_number() over (partition by cover_owner order by cover_start_date) as first_seen
  from cover_base
  where cover_start_date >= timestamp '{{Start Date}}'
    and cover_start_date < timestamp '{{End Date}}'
)

select
  date_trunc('month', cover_start_date) as month_date,
  if(first_seen=1, 'new buyer', 'returning buyer') as buyer_type,
  count(distinct cover_owner) as cnt_buyers,
  count(distinct cover_id) as cnt_covers,
  sum(if('{{display_currency}}' = 'USD', usd_cover_amount, eth_cover_amount)) as total_cover_amount
from cover_totals
group by 1, 2
order by 1
