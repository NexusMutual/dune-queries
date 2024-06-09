select
  cast(json_extract_scalar(t.product_data, '$.id') as int) as product_id,
  cast(json_extract_scalar(t.product_data, '$.productType') as int) as product_type_id,
  cast(json_extract_scalar(t.product_data, '$.name') as varchar) as name,
  cast(json_extract(t.product_data, '$.coverAssets') as array(varchar)) as cover_assets,
  cast(json_extract_scalar(t.product_data, '$.isDeprecated') as boolean) as is_deprecated,
  cast(json_extract_scalar(t.product_data, '$.isPrivate') as boolean) as is_private,
  cast(json_extract_scalar(t.product_data, '$.useFixedPrice') as boolean) as use_fixed_price,
  cast(json_extract_scalar(t.product_data, '$.timestamp') as bigint) as timestamp,
  cast(json_extract(t.product_data, '$.metadata') as json) as metadata
from unnest(cast(json_parse(http_get('https://sdk.nexusmutual.io/data/products.json')) as array(json))) t(product_data)
order by 1
