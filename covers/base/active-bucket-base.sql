select
  cover_id,
  cover_start_time,
  cover_end_time,
  cover_start_date,
  cover_end_date,
  cover_owner,
  staking_pool_id,
  product_id,
  product_type,
  product_name,
  partial_cover_amount as nxm_cover_amount_per_pool,
  sum(partial_cover_amount) over (partition by cover_id) as nxm_cover_amount_total
from query_4599092 -- covers v2 - base root (fallback query)
where cover_end_time >= from_unixtime(28.0 * 86400.0 * cast(floor(date_diff('day', from_unixtime(0), now()) / 28.0) as double)) -- bucket_expiry_date
