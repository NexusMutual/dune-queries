with

products as (
  select
    p.product_id,
    p.product_name,
    pt.product_type_id,
    pt.product_type_name as product_type
  from nexusmutual_ethereum.product_types_v2 pt
    inner join nexusmutual_ethereum.products_v2 p on pt.product_type_id = p.product_type_id
),

covers as (
  select
    ocq.blockchain,
    ocq.quote_id as cover_id,
    ocq.quote_submitted_sender as cover_owner,
    ocq.quote_submitted_block_time,
    ocq.quote_settled_block_time,
    ocq.cover_expiry,
    date_add('day', -1 * ocq.cover_expiry, from_unixtime(ocq.cover_expires_at)) as cover_start_time,
    from_unixtime(ocq.cover_expires_at) as cover_end_time,
    date_trunc('day', date_add('day', -1 * ocq.cover_expiry, from_unixtime(ocq.cover_expires_at))) as cover_start_date,
    date_trunc('day', from_unixtime(ocq.cover_expires_at)) as cover_end_date,
    ocq.provider_id,
    ocq.product_id,
    p.product_type,
    p.product_name,
    ocq.cover_asset,
    ocq.cover_amount,
    if(ocq.cover_asset = 'ETH', ocq.cover_amount, 0) as eth_cover_amount,
    if(ocq.cover_asset = 'DAI', ocq.cover_amount, 0) as dai_cover_amount,
    if(ocq.cover_asset = 'USDC', ocq.cover_amount, 0) as usdc_cover_amount,
    if(ocq.cover_asset = 'cbBTC', ocq.cover_amount, 0) as cbbtc_cover_amount
  from query_3933198 ocq -- opencover - quote events
    left join products p on ocq.product_id = p.product_id
  where ocq.quote_settled_block_time is not null
    and ocq.quote_refunded_block_time is null
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2023-05-30', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_active_covers as (
  select
    ds.block_date,
    count(distinct c.cover_id) as active_cover_count,
    sum(c.eth_cover_amount) as eth_cover_total,
    sum(c.dai_cover_amount) as dai_cover_total,
    sum(c.usdc_cover_amount) as usdc_cover_total,
    sum(c.cbbtc_cover_amount) as cbbtc_cover_total
  from day_sequence ds
    left join covers c on ds.block_date between c.cover_start_date and c.cover_end_date
  group by 1
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    coalesce(decimals, 18) as decimals,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2019-07-12'
  group by 1, 2
),

daily_avg_dai_prices as (
  select
    date_trunc('day', minute) as block_date,
    coalesce(decimals, 18) as decimals,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'DAI'
    and blockchain = 'ethereum'
    and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f
    and minute >= timestamp '2019-07-12'
  group by 1, 2
),

daily_avg_usdc_prices as (
  select
    date_trunc('day', minute) as block_date,
    coalesce(decimals, 18) as decimals,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'USDC'
    and blockchain = 'ethereum'
    and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
    and minute >= timestamp '2019-07-12'
  group by 1, 2
),

daily_avg_cbbtc_prices as (
  select
    date_trunc('day', minute) as block_date,
    coalesce(decimals, 18) as decimals,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'cbBTC'
    and blockchain = 'ethereum'
    and contract_address = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf
    and minute >= timestamp '2024-10-21'
  group by 1, 2
),

daily_active_covers_enriched as (
  select
    ac.block_date,
    ac.active_cover_count,
    --ETH
    ac.eth_cover_total / power(10, p_avg_eth.decimals) as eth_cover_total,
    ac.eth_cover_total / power(10, p_avg_eth.decimals) * p_avg_eth.price_usd as eth_usd_cover_total,
    --DAI
    ac.dai_cover_total / power(10, p_avg_dai.decimals) * p_avg_dai.price_usd / p_avg_eth.price_usd as dai_eth_cover_total,
    ac.dai_cover_total / power(10, p_avg_dai.decimals) * p_avg_dai.price_usd as dai_usd_cover_total,
    --USDC
    ac.usdc_cover_total / power(10, p_avg_usdc.decimals) * p_avg_usdc.price_usd / p_avg_eth.price_usd as usdc_eth_cover_total,
    ac.usdc_cover_total / power(10, p_avg_usdc.decimals) * p_avg_usdc.price_usd as usdc_usd_cover_total,
    --cbBTC
    ac.cbbtc_cover_total / power(10, p_avg_cbbtc.decimals) * p_avg_cbbtc.price_usd / p_avg_eth.price_usd as cbbtc_eth_cover_total,
    ac.cbbtc_cover_total / power(10, p_avg_cbbtc.decimals) * p_avg_cbbtc.price_usd as cbbtc_usd_cover_total
  from daily_active_covers ac
    inner join daily_avg_eth_prices p_avg_eth on ac.block_date = p_avg_eth.block_date
    inner join daily_avg_dai_prices p_avg_dai on ac.block_date = p_avg_dai.block_date
    inner join daily_avg_usdc_prices p_avg_usdc on ac.block_date = p_avg_usdc.block_date
    left join daily_avg_cbbtc_prices p_avg_cbbtc on ac.block_date = p_avg_cbbtc.block_date
)

select
  block_date,
  active_cover_count,
  eth_usd_cover_total,
  dai_usd_cover_total,
  usdc_usd_cover_total,
  cbbtc_usd_cover_total,
  eth_usd_cover_total + dai_usd_cover_total + usdc_usd_cover_total as usd_cover_total
from daily_active_covers_enriched
order by 1 desc
