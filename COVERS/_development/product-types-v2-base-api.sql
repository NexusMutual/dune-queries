select
  cast(json_extract_scalar(t.product_type_data, '$.id') as int) as product_type_id,
  cast(json_extract_scalar(t.product_type_data, '$.name') as varchar) as name,
  cast(json_extract_scalar(t.product_type_data, '$.claimMethod') as int) as claim_method,
  cast(json_extract_scalar(t.product_type_data, '$.gracePeriod') as int) as grace_period,
  cast(json_extract_scalar(t.product_type_data, '$.coverWordingURL') as varchar) as cover_wording_url
from unnest(cast(json_parse(http_get('https://sdk.nexusmutual.io/data/product-types.json')) as array(json))) t(product_type_data)
order by 1
