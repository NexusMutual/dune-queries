with

covers as (
  select
    cover_id,
    date_trunc('day', cover_start_time) as cover_start_date,
    date_trunc('day', cover_end_time) as cover_end_date,
    cover_asset,
    sum_assured,
    cover_owner
  from query_3788367 -- covers v1 base (fallback) query
  union all
  select distinct
    cover_id,
    date_trunc('day', cover_start_time) as cover_start_date,
    date_trunc('day', cover_end_time) as cover_end_date,
    cover_asset,
    sum_assured,
    cover_owner
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

owners as (
  select
    cover_owner,
    sum(
      case
        when '{{display_currency}}' = 'USD'
        AND cover_asset = 'DAI' then dai_price_dollar * sum_assured
        when '{{display_currency}}' = 'USD'
        AND cover_asset = 'ETH' then eth_price_dollar * sum_assured
        when '{{display_currency}}' = 'ETH'
        AND cover_asset = 'DAI' then dai_price_dollar * sum_assured / eth_price_dollar
        when '{{display_currency}}' = 'ETH'
        AND cover_asset = 'ETH' then sum_assured
        ELSE -1
      END
    ) as total_cover
  from covers c
    inner join daily_avg_eth_prices p_avg_eth on c.cover_start_date = p_avg_eth.block_date
    inner join daily_avg_dai_prices p_avg_dai on c.cover_start_date = p_avg_dai.block_date
  group by 1
)

select
  owner,
  count(owner) over () as unique_users,
  total_cover
from owners
