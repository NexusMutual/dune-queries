with

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount
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
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount
  --from query_3788370 -- covers v2 base (fallback) query
  from nexusmutual_ethereum.covers_v2
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
    sum(c.usdc_cover_amount) as usdc_cover_total
  from day_sequence ds
    left join covers c on ds.block_date between c.cover_start_date and c.cover_end_date
  group by 1
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2019-07-12'
  group by 1
),

daily_avg_dai_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'DAI'
    and blockchain = 'ethereum'
    and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f
    and minute >= timestamp '2019-07-12'
  group by 1
),

daily_avg_usdc_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'USDC'
    and blockchain = 'ethereum'
    and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
    and minute >= timestamp '2019-07-12'
  group by 1
),

daily_active_covers_enriched as (
  select
    ac.block_date,
    --ETH
    ac.eth_cover_total,
    ac.eth_cover_total * p_avg_eth.price_usd as eth_usd_cover_total,
    --DAI
    ac.dai_cover_total * p_avg_dai.price_usd / p_avg_eth.price_usd as dai_eth_cover_total,
    ac.dai_cover_total * p_avg_dai.price_usd as dai_usd_cover_total,
    --USDC
    ac.usdc_cover_total * p_avg_usdc.price_usd / p_avg_eth.price_usd as usdc_eth_cover_total,
    ac.usdc_cover_total * p_avg_usdc.price_usd as usdc_usd_cover_total
  from daily_active_covers ac
    inner join daily_avg_eth_prices p_avg_eth on ac.block_date = p_avg_eth.block_date
    inner join daily_avg_dai_prices p_avg_dai on ac.block_date = p_avg_dai.block_date
    inner join daily_avg_usdc_prices p_avg_usdc on ac.block_date = p_avg_usdc.block_date
)

select
  block_date,
  sum(if('{{display_currency}}' = 'USD', eth_usd_cover_total, eth_cover_total)) as eth_cover_total,
  sum(if('{{display_currency}}' = 'USD', dai_usd_cover_total, dai_eth_cover_total)) as dai_cover_total,
  sum(if('{{display_currency}}' = 'USD', usdc_usd_cover_total, usdc_eth_cover_total)) as usdc_cover_total,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_total + dai_usd_cover_total + usdc_usd_cover_total,
    eth_cover_total + dai_eth_cover_total + usdc_eth_cover_total
  )) as cover_total
from daily_active_covers_enriched
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
group by 1
order by 1 desc
