with

active_covers as (
  select
    cover_id,
    cover_owner,
    cover_start_time,
    cover_end_time,
    cover_asset,
    sum_assured,
    --ETH
    eth_cover_amount,
    eth_usd_cover_amount,
    --DAI
    dai_eth_cover_amount,
    dai_usd_cover_amount,
    --USDC
    usdc_eth_cover_amount,
    usdc_usd_cover_amount,
    --cbBTC
    cbbtc_eth_cover_amount,
    cbbtc_usd_cover_amount
  --from query_3834200 -- active covers base (fallback) query
  from nexusmutual_ethereum.active_covers
  where cover_end_time <= case '{{expiry within}}'
      when '2 weeks' then current_date + interval '14' day
      when '1 month' then current_date + interval '1' month
      when '2 months' then current_date + interval '2' month
      when '3 months' then current_date + interval '3' month
      when '6 months' then current_date + interval '6' month
      when 'no end date' then (select cast(max(cover_end_time) as timestamp) from query_4599092)
    end
),

covers_agg as (
  select
    cover_id,
    cover_owner,
    cover_end_time,
    cover_asset,
    sum_assured as cover_amount,
    sum(eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount) as usd_cover_amount
  from active_covers
  group by 1, 2, 3, 4, 5
)

select
  c.cover_id,
  coalesce(ens.name, cast(c.cover_owner as varchar)) as cover_owner,
  c.cover_end_time,
  c.cover_asset,
  c.cover_amount,
  c.usd_cover_amount
from covers_agg c
  left join labels.ens on c.cover_owner = ens.address
order by usd_cover_amount desc
