select
  blockchain,
  cover_id,
  cover_start_time,
  cover_end_time,
  product_type,
  product_name,
  cover_asset,
  cover_amount,
  usd_cover_amount,
  payment_asset,
  premium_amount,
  usd_premium_amount,
  cover_owner
from query_3950051 -- opencover - quote base
order by 1, 2 desc
