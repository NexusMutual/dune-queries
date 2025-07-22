with

cover_owners as (
  select
    cover_owner,
    cover_sold,
    usd_cover,
    usd_premium,
    mean_usd_cover,
    median_usd_cover,
    case
      when median_usd_cover < 10000 then 1
      when median_usd_cover < 100000 then 2
      else 3
    end as bucket_rn,
    case
      when median_usd_cover < 10000 then '0-10k'
      when median_usd_cover < 100000 then '10k-100k'
      else '100k+'
    end as bucket
  --from query_3913267 -- BD cover owners base
  from nexusmutual_ethereum.cover_owners_agg
)

select
  bucket_rn,
  bucket,
  sum(cover_sold) as cover_count,
  sum(usd_cover) as usd_cover,
  sum(usd_premium) as usd_premium
from cover_owners
group by 1, 2
order by 1
