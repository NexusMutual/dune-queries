with

-- cte as not possible to ref from query_xxxx
product_api as (
  select
    cast(json_extract_scalar(t.product_data, '$.id') as int) as product_id,
    cast(json_extract_scalar(t.product_data, '$.productType') as int) as product_type_id,
    cast(json_extract_scalar(t.product_data, '$.name') as varchar) as product_name,
    array_join(cast(json_extract(t.product_data, '$.coverAssets') as array(varchar)), ', ') as cover_assets,
    cast(json_extract_scalar(t.product_data, '$.isDeprecated') as boolean) as is_deprecated,
    cast(json_extract_scalar(t.product_data, '$.isPrivate') as boolean) as is_private,
    cast(json_extract_scalar(t.product_data, '$.useFixedPrice') as boolean) as use_fixed_price,
    cast(json_extract_scalar(t.product_data, '$.timestamp') as bigint) as timestamp,
    cast(json_extract(t.product_data, '$.metadata') as json) as metadata
  from unnest(cast(json_parse(http_get('https://sdk.nexusmutual.io/data/products.json')) as array(json))) t(product_data)
)

-- check against query
select p.product_id, p.product_type_id, p.product_name, p.cover_assets, p_api.cover_assets as api_cover_assets
from product_api p_api
  left join query_3788363 p -- products
    on p_api.product_id = p.product_id
where coalesce(p_api.product_type_id, 0) <> coalesce(p.product_type_id, 0)
  or coalesce(p_api.product_name, '') <> coalesce(p.product_name, '')
  or coalesce(p_api.cover_assets, '') <> coalesce(p.cover_assets, '')
order by 1

/*
-- alternative check against spell
select product_id, product_type_id, product_name, cover_assets
from products_api
except
select product_id, product_type_id, product_name, cover_assets
from nexusmutual_ethereum.products_v2
*/
