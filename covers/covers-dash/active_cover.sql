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
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', sum_assured, 0) as cbbtc_cover_amount
  from query_4599092 -- covers v2 - base ref (fallback query)
  where is_migrated = false
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2019-07-12', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_active_covers as (
  select
    ds.block_date,
    sum(c.eth_cover_amount) as eth_cover_total,
    sum(c.dai_cover_amount) as dai_cover_total,
    sum(c.usdc_cover_amount) as usdc_cover_total,
    sum(c.cbbtc_cover_amount) as cbbtc_cover_total
  from day_sequence ds
    left join covers c on ds.block_date between c.cover_start_date and c.cover_end_date
  group by 1
),

daily_active_covers_enriched as (
  select
    ac.block_date,
    --ETH
    ac.eth_cover_total,
    ac.eth_cover_total * p.avg_eth_usd_price as eth_usd_cover_total,
    --DAI
    ac.dai_cover_total * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_cover_total,
    ac.dai_cover_total * p.avg_dai_usd_price as dai_usd_cover_total,
    --USDC
    ac.usdc_cover_total * p.avg_usdc_usd_price / p.avg_eth_usd_price as usdc_eth_cover_total,
    ac.usdc_cover_total * p.avg_usdc_usd_price as usdc_usd_cover_total,
    --cbBTC
    ac.cbbtc_cover_total * p.avg_cbbtc_usd_price / p.avg_eth_usd_price as cbbtc_eth_cover_total,
    ac.cbbtc_cover_total * p.avg_cbbtc_usd_price as cbbtc_usd_cover_total
  from daily_active_covers ac
    inner join daily_avg_prices p on ac.block_date = p.block_date
)

select
  block_date,
  sum(if('{{display_currency}}' = 'USD', eth_usd_cover_total, eth_cover_total)) as eth_cover_total,
  sum(if('{{display_currency}}' = 'USD', dai_usd_cover_total, dai_eth_cover_total)) as dai_cover_total,
  sum(if('{{display_currency}}' = 'USD', usdc_usd_cover_total, usdc_eth_cover_total)) as usdc_cover_total,
  sum(if('{{display_currency}}' = 'USD', cbbtc_usd_cover_total, cbbtc_eth_cover_total)) as cbbtc_cover_total,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_total + dai_usd_cover_total + usdc_usd_cover_total + cbbtc_usd_cover_total,
    eth_cover_total + dai_eth_cover_total + usdc_eth_cover_total + cbbtc_eth_cover_total
  )) as cover_total
from daily_active_covers_enriched
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
group by 1
order by 1 desc
