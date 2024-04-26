with

product_v1_to_v2_mapping as (
  select
    legacyProductId as product_contract_address,
    output_0 as product_id_v2
  from nexusmutual_ethereum.ProductsV1_call_getNewProductId
  where call_success
  group by 1, 2
),

product_info as (
  select
    i.product_contract_address as product_address,
    i.syndicate,
    i.product_name,
    i.product_type,
    m.product_id_v2
  from nexusmutual_ethereum.product_information i
    inner join product_v1_to_v2_mapping m on i.product_contract_address = m.product_contract_address
),

product_data_raw as (
  select
    p.call_block_time as block_time,
    p.call_block_number as block_number,
    p.productParams,
    cast(json_extract_scalar(t.product_param, '$.productId') as uint256) as product_id,
    json_extract_scalar(t.product_param, '$.productName') as product_name,
    json_extract_scalar(t.product_param, '$.ipfsMetadata') as product_ipfs_metadata,
    json_extract(t.product_param, '$.allowedPools') as allowed_pools,
    json_parse(json_query(t.product_param, 'lax $.product' omit quotes)) as product_json,
    t.ord as product_ordinality,
    p.call_tx_hash as tx_hash
  from nexusmutual_ethereum.Cover_call_setProducts p
    cross join unnest (p.productParams) with ordinality as t(product_param, ord)
  where p.call_success
    and p.contract_address = 0xcafeac0ff5da0a2777d915531bfa6b29d282ee62
),

product_data as (
  select
    block_time,
    block_number,
    product_id,
    product_name,
    cast(json_extract_scalar(product_json, '$.productType') as int) as product_type,
    try_cast(json_extract_scalar(product_json, '$.capacityReductionRatio') as int) as capacity_reduction_ratio,
    try_cast(json_extract_scalar(product_json, '$.coverAssets') as int) as cover_assets,
    try_cast(json_extract_scalar(product_json, '$.initialPriceRatio') as int) as initial_price_ratio,
    cast(json_extract_scalar(product_json, '$.isDeprecated') as boolean) as is_deprecated,
    cast(json_extract_scalar(product_json, '$.useFixedPrice') as boolean) as use_fixed_price,
    json_extract_scalar(product_json, '$.yieldTokenAddress') as yield_token_address,
    product_ipfs_metadata,
    allowed_pools,
    product_ordinality,
    tx_hash,
    row_number() over (order by block_time, product_ordinality) as generated_product_id
  from product_data_raw
  --where product_id > 1000000000000
),

product_type_data_raw as (
  select distinct
    pt.call_block_time as block_time,
    pt.call_block_number as block_number,
    pt.productTypeParams,
    cast(json_extract_scalar(t.product_type_param, '$.productTypeId') as uint256) as product_type_id,
    json_extract_scalar(t.product_type_param, '$.productTypeName') as product_type_name,
    json_extract_scalar(t.product_type_param, '$.ipfsMetadata') as product_type_ipfs_metadata,
    json_parse(json_query(t.product_type_param, 'lax $.productType' omit quotes)) as product_type_json,
    t.ord as product_type_ordinality,
    pt.call_tx_hash as tx_hash
  from nexusmutual_ethereum.Cover_call_setProductTypes pt
    cross join unnest (pt.productTypeParams) with ordinality as t(product_type_param, ord)
  where call_success
),

product_type_data as (
  select
    block_time,
    block_number,
    product_type_id, --product_type_id_input
    product_type_name,
    try_cast(json_extract_scalar(product_type_json, '$.claimMethod') as int) as claim_method,
    try_cast(json_extract_scalar(product_type_json, '$.gracePeriod') as bigint) as grace_period,
    product_type_ipfs_metadata,
    product_type_ordinality,
    tx_hash,
    row_number() over (order by block_time, product_type_ordinality) as generated_product_type_id
  from product_type_data_raw
  where length(product_type_name) > 0
    and product_type_id > 1000000
),

v2_products as (
  SELECT
    product_id,
    product_name,
    a.product_type_id,
    product_type_name
  FROM
    product_set as a
    LEFT JOIN product_types as b ON a.product_type_id = b.product_type_id -- is this join reliable?
  ORDER BY
    product_id
)

/*
TODO:
nexusmutual_ethereum.Cover_evt_ProductSet : evt_index -> call ordinality?
nexusmutual_ethereum.Cover_evt_ProductTypeSet
*/

select
  evt_block_time as block_time,
  evt_block_number as block_number,
  id as product_id,
  ipfsMetadata as product_ipfs_metadata,
  evt_index,
  evt_tx_hash as tx_hash,
  row_number() over (partition by evt_block_time, evt_tx_hash order by evt_index) as rn
from nexusmutual_ethereum.Cover_evt_ProductSet
order by 1, rn
