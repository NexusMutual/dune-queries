with

covers as (
  select distinct
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_status,
    product_type,
    product_name,
    cover_asset,
    native_cover_amount,
    cover_start_usd as usd_cover_amount,
    cover_start_eth as eth_cover_amount,
    premium_asset,
    sum(premium_native) over (partition by cover_id) as native_premium,
    sum(premium_nxm) over (partition by cover_id) as nxm_premium,
    sum(premium_usd) over (partition by cover_id) as usd_premium,
    cover_owner,
    cover_period
  from query_3810247 -- full list covers v2
),

cover_buckets as (
  select
    case
      when usd_cover_amount < 10000 then 1
      when usd_cover_amount < 100000 then 2
      else 3
    end as bucket_rn,
    case
      when usd_cover_amount < 10000 then '0-10k'
      when usd_cover_amount < 100000 then '10k-100k'
      else '100k+'
    end as bucket,
    count(*) as cover_count,
    sum(usd_cover_amount) as usd_cover_amount,
    sum(usd_premium) as usd_premium
  from covers
  group by 1, 2
)

select
  bucket,
  cover_count,
  usd_cover_amount,
  usd_premium
from cover_buckets
order by bucket_rn
