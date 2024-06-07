with

covers as (
  select
    cover_id,
    date_trunc('day', cover_start_time) as cover_start_date,
    date_trunc('day', cover_end_time) as cover_end_date,
    cover_asset,
    sum_assured
  from query_3788367 -- covers v1 base (fallback) query
  union all
  select distinct
    cover_id,
    date_trunc('day', cover_start_time) as cover_start_date,
    date_trunc('day', cover_end_time) as cover_end_date,
    cover_asset,
    sum_assured
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

total_cover_underwritten as (
  select
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    p_avg_eth.price_usd as avg_eth_usd_price,
    p_avg_dai.price_usd as avg_dai_usd_price,
    coalesce(if(c.cover_asset = 'ETH', c.sum_assured, 0), 0) as eth_cover_amount,
    coalesce(if(c.cover_asset = 'ETH', c.sum_assured, 0) * p_avg_eth.price_usd, 0) as eth_usd_cover_amount,
    coalesce(if(c.cover_asset = 'DAI', c.sum_assured, 0) * p_avg_dai.price_usd / p_avg_eth.price_usd, 0) as dai_eth_cover_amount,
    coalesce(if(c.cover_asset = 'DAI', c.sum_assured, 0) * p_avg_dai.price_usd, 0) as dai_usd_cover_amount
  from covers c
    inner join daily_avg_eth_prices p_avg_eth on c.cover_start_date = p_avg_eth.block_date
    inner join daily_avg_dai_prices p_avg_dai on c.cover_start_date = p_avg_dai.block_date
)

select
  sum(if('{{display_currency}}' = 'USD', eth_usd_cover_amount, eth_cover_amount)) as total_eth_cover_amount,
  sum(if('{{display_currency}}' = 'USD', dai_usd_cover_amount, dai_eth_cover_amount)) as total_dai_cover_amount,
  sum(if('{{display_currency}}' = 'USD', eth_usd_cover_amount + dai_usd_cover_amount, eth_cover_amount + dai_eth_cover_amount)) as total_cover_amount
from total_cover_underwritten
where cover_start_date between timestamp '{{Start Date}}' and timestamp '{{End Date}}'
  or cover_end_date between timestamp '{{Start Date}}' and timestamp '{{End Date}}'
