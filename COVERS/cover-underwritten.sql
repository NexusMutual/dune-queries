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
  from query_3788367 -- covers v1 base (fallback) query
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
  from query_3788370 -- covers v2 base (fallback) query
  where is_migrated = false
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2019-05-01'
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

total_cover_underwritten as (
  select
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    p_avg_eth.price_usd as avg_eth_usd_price,
    p_avg_dai.price_usd as avg_dai_usd_price,
    --ETH
    c.eth_cover_amount,
    c.eth_cover_amount * p_avg_eth.price_usd as eth_usd_cover_amount,
    --DAI
    c.dai_cover_amount * p_avg_dai.price_usd / p_avg_eth.price_usd as dai_eth_cover_amount,
    c.dai_cover_amount * p_avg_dai.price_usd as dai_usd_cover_amount,
    --USDC
    c.usdc_cover_amount * p_avg_usdc.price_usd / p_avg_eth.price_usd as usdc_eth_cover_amount,
    c.usdc_cover_amount * p_avg_usdc.price_usd as usdc_usd_cover_amount
  from covers c
    inner join daily_avg_eth_prices p_avg_eth on c.cover_start_date = p_avg_eth.block_date
    inner join daily_avg_dai_prices p_avg_dai on c.cover_start_date = p_avg_dai.block_date
    inner join daily_avg_usdc_prices p_avg_usdc on c.cover_start_date = p_avg_usdc.block_date
)

select
  sum(if('{{display_currency}}' = 'USD', eth_usd_cover_amount, eth_cover_amount)) as total_eth_cover_amount,
  sum(if('{{display_currency}}' = 'USD', dai_usd_cover_amount, dai_eth_cover_amount)) as total_dai_cover_amount,
  sum(if('{{display_currency}}' = 'USD', usdc_usd_cover_amount, usdc_eth_cover_amount)) as total_usdc_cover_amount,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount
  )) as total_cover_amount
from total_cover_underwritten
where cover_start_date >= timestamp '{{Start Date}}'
  and cover_start_date < timestamp '{{End Date}}'
