with

payment_assets (blockchain, symbol, contract_address) as (
  values
  ('arbitrum', 'DAI', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1),
  ('arbitrum', 'USDC', 0xaf88d065e77c8cc2239327c5edb3a432268e5831),
  ('arbitrum', 'USDT', 0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9),
  ('arbitrum', 'WETH', 0x82af49447d8a07e3bd95bd0d56f35241523fbab1),
  ('arbitrum', 'cbBTC', 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf),
  ('base', 'DAI', 0x50c5725949a6f0c72e6c4a641f24049a917db0cb),
  ('base', 'USDC', 0x833589fcd6edb6e08f4c7c32d4f71b54bda02913),
  ('base', 'WETH', 0x4200000000000000000000000000000000000006),
  ('base', 'cbBTC', 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf),
  ('optimism', 'DAI', 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1),
  ('optimism', 'USDC', 0x0b2c639c533813f4aa9d7837caf62653d097ff85),
  ('optimism', 'USDT', 0x94b008aa00579c1307b0ef2c499ad98a8ce58e58),
  ('optimism', 'WETH', 0x4200000000000000000000000000000000000006),
  ('optimism', 'cbBTC', 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf), -- ???
  ('polygon', 'DAI', 0x8f3cf7ad23cd3cadbd9735aff958023239c6a063),
  ('polygon', 'MATIC', 0x0000000000000000000000000000000000001010),
  ('polygon', 'USDC', 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359),
  ('polygon', 'USDT', 0xc2132d05d31c914a87c6611c10748aeb04b58e8f),
  ('polygon', 'WETH', 0x7ceb23fd6bc0add59e62ac25578270cff1b9f619),
  ('polygon', 'cbBTC', 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf) -- ???
),

daily_avg_prices as (
  select
    date_trunc('day', p.minute) as block_date,
    p.blockchain,
    if(p.symbol = 'WETH', 'ETH', p.symbol) as asset_symbol,
    p.decimals as asset_decimals,
    avg(p.price) as price_usd
  from prices.usd p
    inner join payment_assets pa on p.blockchain = pa.blockchain and p.symbol = pa.symbol and p.contract_address = pa.contract_address
  where p.minute >= timestamp '2023-05-30'
  group by 1, 2, 3, 4
),

products as (
  select
    p.product_id,
    p.product_name,
    pt.product_type_id,
    pt.product_type_name as product_type
  from nexusmutual_ethereum.product_types_v2 pt
    inner join nexusmutual_ethereum.products_v2 p on pt.product_type_id = p.product_type_id
)

select
  ocq.blockchain,
  ocq.quote_id as cover_id,
  ocq.quote_submitted_sender as cover_owner,
  ocq.quote_submitted_block_time,
  ocq.quote_settled_block_time,
  ocq.cover_expiry,
--   date_add('day', -1 * ocq.cover_expiry, from_unixtime(ocq.cover_expires_at)) as cover_start_time,
--   from_unixtime(ocq.cover_expires_at) as cover_end_time,
--   date_trunc('day', date_add('day', -1 * ocq.cover_expiry, from_unixtime(ocq.cover_expires_at))) as cover_start_date,
--   date_trunc('day', from_unixtime(ocq.cover_expires_at)) as cover_end_date,
  quote_settled_block_time as cover_start_time,
  date_add('day', ocq.cover_expiry, ocq.quote_settled_block_time) as cover_end_time,
  date_trunc('day', quote_settled_block_time) as cover_start_date,
  date_trunc('day', date_add('day', ocq.cover_expiry, ocq.quote_settled_block_time)) as cover_end_date,
  ocq.provider_id,
  ocq.product_id % 10000 as product_id,
  p.product_type,
  p.product_name,
  ocq.cover_asset,
  ocq.cover_amount / power(10, pca.asset_decimals) as cover_amount,
  ocq.cover_amount / power(10, pca.asset_decimals) * pca.price_usd as usd_cover_amount,
  ocq.payment_asset,
  ocq.premium_amount / power(10, ppa.asset_decimals) as premium_amount,
  ocq.premium_amount / power(10, ppa.asset_decimals) * ppa.price_usd as usd_premium_amount,
  ocq.fee_amount
from query_3933198 ocq -- opencover - quote events
  inner join daily_avg_prices pca on ocq.blockchain = pca.blockchain
    and ocq.quote_submitted_block_date = pca.block_date
    and ocq.cover_asset = pca.asset_symbol
  inner join daily_avg_prices ppa on ocq.blockchain = ppa.blockchain
    and ocq.quote_submitted_block_date = ppa.block_date
    and ocq.payment_asset = ppa.asset_symbol
  left join products p on ocq.product_id % 10000 = p.product_id
where ocq.quote_settled_block_time is not null
  and ocq.quote_refunded_block_time is null
--order by 1,2
