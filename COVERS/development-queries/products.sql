with

product_events as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    id as product_id,
    ipfsMetadata as evt_product_ipfs_metadata,
    evt_index,
    evt_tx_hash as tx_hash,
    row_number() over (partition by evt_block_time, evt_tx_hash order by evt_index) as evt_rn
  from nexusmutual_ethereum.Cover_evt_ProductSet
),

product_calls as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    productParams,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_time, call_tx_hash order by call_trace_address desc) as tx_call_rn
  from nexusmutual_ethereum.Cover_call_setProducts
  where call_success
    and contract_address = 0xcafeac0ff5da0a2777d915531bfa6b29d282ee62
),

product_data_raw as (
  select
    p.block_time,
    p.block_number,
    p.productParams,
    cast(json_extract_scalar(t.product_param, '$.productId') as uint256) as product_id,
    json_extract_scalar(t.product_param, '$.productName') as product_name,
    json_extract_scalar(t.product_param, '$.ipfsMetadata') as call_product_ipfs_metadata,
    json_extract(t.product_param, '$.allowedPools') as allowed_pools,
    json_parse(json_query(t.product_param, 'lax $.product' omit quotes)) as product_json,
    t.product_ordinality,
    p.tx_hash
  from product_calls p
    cross join unnest (p.productParams) with ordinality as t(product_param, product_ordinality)
  where p.tx_call_rn = 1
),

product_data as (
  select
    block_time,
    block_number,
    product_id,
    product_name,
    cast(json_extract_scalar(product_json, '$.productType') as int) as product_type_id,
    try_cast(json_extract_scalar(product_json, '$.capacityReductionRatio') as int) as capacity_reduction_ratio,
    try_cast(json_extract_scalar(product_json, '$.coverAssets') as int) as cover_assets,
    try_cast(json_extract_scalar(product_json, '$.initialPriceRatio') as int) as initial_price_ratio,
    cast(json_extract_scalar(product_json, '$.isDeprecated') as boolean) as is_deprecated,
    cast(json_extract_scalar(product_json, '$.useFixedPrice') as boolean) as use_fixed_price,
    from_hex(json_extract_scalar(product_json, '$.yieldTokenAddress')) as yield_token_address,
    call_product_ipfs_metadata,
    allowed_pools,
    product_ordinality,
    tx_hash,
    case
      when product_id < 1000000 then null
      else row_number() over (partition by block_time, tx_hash, if(product_id < 1000000, 0, 1) order by product_ordinality)
    end as call_rn
  from product_data_raw
),

products_ext as (
  select
    e.block_time,
    e.block_number,
    e.product_id,
    c.product_name,
    c.product_type_id,
    c.capacity_reduction_ratio,
    c.cover_assets,
    c.initial_price_ratio,
    c.is_deprecated,
    c.use_fixed_price,
    c.yield_token_address,
    c.allowed_pools,
    coalesce(e.evt_product_ipfs_metadata, c.call_product_ipfs_metadata) as product_ipfs_metadata,
    e.evt_index,
    e.tx_hash
  from product_events e
    inner join product_data c on e.block_time = c.block_time and e.block_number = c.block_number and e.evt_rn = c.call_rn
  union all
  select
    c.block_time,
    c.block_number,
    c.product_id,
    c.product_name,
    c.product_type_id,
    c.capacity_reduction_ratio,
    c.cover_assets,
    c.initial_price_ratio,
    c.is_deprecated,
    c.use_fixed_price,
    c.yield_token_address,
    c.allowed_pools,
    c.call_product_ipfs_metadata as product_ipfs_metadata,
    cast(0 as bigint) as evt_index,
    c.tx_hash
  from product_data c
    left join product_events e on e.block_time = c.block_time and e.block_number = c.block_number and e.evt_rn = c.call_rn
  where e.block_number is null
),

products as (
  select
    block_time,
    block_number,
    product_type_id,
    product_id,
    product_name,
    case cover_assets
      when 0 then 'ETH/DAI'
      when 1 then 'ETH'
      when 2 then 'DAI'
      else 'unkown'
    end as cover_assets,
    is_deprecated,
    initial_price_ratio,
    use_fixed_price,
    capacity_reduction_ratio,
    allowed_pools,
    yield_token_address,
    product_ipfs_metadata,
    evt_index,
    tx_hash,
    row_number() over (partition by product_id order by block_time desc) as rn
  from products_ext
)

select
  block_time,
  block_number,
  product_type_id,
  product_id,
  product_name,
  cover_assets,
  is_deprecated,
  initial_price_ratio,
  use_fixed_price,
  capacity_reduction_ratio,
  allowed_pools,
  yield_token_address,
  product_ipfs_metadata,
  evt_index,
  tx_hash
from products
where rn = 1
--order by product_id