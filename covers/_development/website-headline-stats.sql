-- Website headline stats: one row, no parameters.
-- Serves nexusmutual.io marketing pages through the dune-proxy (/v2/dune?queryId=<id>).
-- Columns: cover_underwritten_usd (all time, USD at cover start date),
--          cover_count (v1 + non-migrated v2, same de-dup rule as covers-dash),
--          active_cover_usd (currently active covers at latest prices).
-- After saving on Dune: add the query id to dune-proxy QUERY_IDS.

with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_cbbtc_usd_price
  from nexusmutual_ethereum.capital_pool_prices
),

latest_prices as (
  select
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_cbbtc_usd_price
  from nexusmutual_ethereum.capital_pool_prices
  order by block_date desc
  limit 1
),

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    cast(0 as double) as usdc_cover_amount,
    cast(0 as double) as cbbtc_cover_amount
  from nexusmutual_ethereum.covers_v1
  union all
  select distinct
    cover_id,
    cover_start_date,
    cover_end_date,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', sum_assured, 0) as cbbtc_cover_amount
  from query_4599092 -- covers v2 - base root (fallback query)
  where is_migrated = false
),

underwritten as (
  select
    count(*) as cover_count,
    sum(
      c.eth_cover_amount * p.avg_eth_usd_price
      + c.dai_cover_amount * p.avg_dai_usd_price
      + c.usdc_cover_amount * p.avg_usdc_usd_price
      + c.cbbtc_cover_amount * p.avg_cbbtc_usd_price
    ) as cover_underwritten_usd
  from covers c
    inner join daily_avg_prices p on c.cover_start_date = p.block_date
),

active as (
  select
    sum(
      c.eth_cover_amount * lp.avg_eth_usd_price
      + c.dai_cover_amount * lp.avg_dai_usd_price
      + c.usdc_cover_amount * lp.avg_usdc_usd_price
      + c.cbbtc_cover_amount * lp.avg_cbbtc_usd_price
    ) as active_cover_usd
  from covers c
    cross join latest_prices lp
  where current_timestamp between c.cover_start_date and c.cover_end_date
)

select
  u.cover_underwritten_usd,
  u.cover_count,
  a.active_cover_usd
from underwritten u
  cross join active a
