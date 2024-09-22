with

covers as (
  select
    cover_id,
    cast(cover_start_date as date) as cover_start_date,
    cast(cover_end_date as date) as cover_end_date,
    case blockchain
      when 'arbitrum' then '🟢'
      when 'base' then '🔵'
      when 'optimism' then '🔴'
      when 'polygon' then '🟣'
    end blockchain,
    cover_owner,
    product_id,
    product_name,
    product_type,
    usd_cover_amount,
    usd_premium_amount
  from query_3950051 -- opencover - quote base
),

cover_owner_aggs as (
  select
    cover_owner,
    listagg(distinct blockchain, '') within group (order by blockchain) as blockchain,
    count(distinct cover_id) as cover_sold,
    count(distinct coalesce(product_id, -1)) as product_sold,
    min(cover_start_date) as first_cover_buy,
    max(cover_start_date) as last_cover_buy,
    sum(usd_cover_amount) as usd_cover_amount,
    sum(usd_premium_amount) as usd_premium_amount,
    approx_percentile(usd_cover_amount, 0.5) as median_usd_cover,
    approx_percentile(usd_premium_amount, 0.5) as median_usd_premium
  from covers
  group by 1
)

select
  cover_owner,
  blockchain,
  cover_sold,
  product_sold,
  coalesce(1.00 * product_sold / nullif(cover_sold, 0), 0) as mean_product_sold,
  first_cover_buy,
  last_cover_buy,
  usd_cover_amount,
  coalesce(usd_cover_amount / nullif(cover_sold, 0), 0) as mean_usd_cover,
  median_usd_cover,
  usd_premium_amount,
  coalesce(usd_premium_amount / nullif(cover_sold, 0), 0) as mean_usd_premium,
  median_usd_premium
from cover_owner_aggs
order by cover_sold desc
