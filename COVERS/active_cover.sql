with

covers as (
  select
    cover_id,
    date_trunc('day', cover_start_time) as cover_start_date,
    date_trunc('day', cover_end_time) as cover_end_date,
    cover_asset,
    sum_assured,
    coalesce(if(cover_asset = 'ETH', sum_assured, 0), 0) as eth_cover_amount,
    coalesce(if(cover_asset = 'DAI', sum_assured, 0), 0) as dai_cover_amount
  from query_3788367 -- covers v1 base (fallback) query
  union all
  select distinct
    cover_id,
    date_trunc('day', cover_start_time) as cover_start_date,
    date_trunc('day', cover_end_time) as cover_end_date,
    cover_asset,
    sum_assured,
    coalesce(if(cover_asset = 'ETH', sum_assured, 0), 0) as eth_cover_amount,
    coalesce(if(cover_asset = 'DAI', sum_assured, 0), 0) as dai_cover_amount
  from query_3788370 -- covers v2 base (fallback) query
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
    sum(c.dai_cover_amount) as dai_cover_total
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

daily_active_covers_enriched as (
  select
    ac.block_date,
    ac.eth_cover_total,
    ac.eth_cover_total * p_avg_eth.price_usd as eth_usd_cover_total,
    ac.dai_cover_total,
    ac.dai_cover_total * p_avg_dai.price_usd / p_avg_eth.price_usd as dai_eth_cover_total,
    ac.dai_cover_total * p_avg_dai.price_usd as dai_usd_cover_total
  from daily_active_covers ac
    inner join daily_avg_eth_prices p_avg_eth on ac.block_date = p_avg_eth.block_date
    inner join daily_avg_dai_prices p_avg_dai on ac.block_date = p_avg_dai.block_date
)

select
  block_date,
  sum(if('{{display_currency}}' = 'USD', eth_usd_cover_total, eth_cover_total)) as total_eth_cover_total,
  sum(if('{{display_currency}}' = 'USD', dai_usd_cover_total, dai_eth_cover_total)) as total_dai_cover_total,
  sum(if('{{display_currency}}' = 'USD', eth_usd_cover_total + dai_usd_cover_total, eth_cover_total + dai_eth_cover_total)) as total_cover_total
from daily_active_covers_enriched
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
group by 1
order by 1 desc
