with

active_covers as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
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
),

weigthed_cal as (
  select
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount as dollar_total,
    date_diff('day', now(), cover_end_time) * (
      eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount
    ) as day_weighted_dollar_total
  from active_covers
)

select
  sum(day_weighted_dollar_total) / sum(dollar_total) as total_dollar_weighted
from weigthed_cal
