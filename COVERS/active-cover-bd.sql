with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from query_3789851 -- prices base (fallback) query
),

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    syndicate as staking_pool,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured as cover_amount,
    premium_asset,
    premium
  --from query_3788367 -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1
  union all
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    staking_pool,
    product_type,
    product_name,
    cover_asset,
    sum_assured,
    sum_assured * partial_cover_amount / sum(partial_cover_amount) over (partition by cover_id) as cover_amount,
    premium_asset,
    premium_incl_commission as premium
  --from query_3788370 -- covers v2 base (fallback) query
  from nexusmutual_ethereum.covers_v2
  where is_migrated = false
),

covers_ext as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    staking_pool,
    product_type,
    product_name,
    cover_asset,
    if(cover_asset = 'ETH', cover_amount, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', cover_amount, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', cover_amount, 0) as usdc_cover_amount,
    premium_asset,
    if(staking_pool = 'v1' and cover_asset = 'ETH', premium, 0) as eth_premium_amount,
    if(staking_pool = 'v1' and cover_asset = 'DAI', premium, 0) as dai_premium_amount,
    if(staking_pool <> 'v1', premium, 0) as nxm_premium_amount
  from covers
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2019-07-12', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_active_covers as (
  select
    ds.block_date,
    sum(ac.eth_cover_amount) as eth_cover_total,
    sum(ac.dai_cover_amount) as dai_cover_total,
    sum(ac.usdc_cover_amount) as usdc_cover_total,
    sum(fees.eth_premium_amount) as eth_premium_total,
    sum(fees.dai_premium_amount) as dai_premium_total,
    sum(fees.nxm_premium_amount) as nxm_premium_total
  from day_sequence ds
    left join covers_ext ac on ds.block_date between ac.cover_start_date and ac.cover_end_date
    left join covers_ext fees on ds.block_date = fees.cover_start_date
  group by 1
),

daily_active_covers_enriched as (
  select
    ac.block_date,
    --== covers ==
    --ETH
    coalesce(ac.eth_cover_total, 0) as eth_eth_cover_total,
    coalesce(ac.eth_cover_total * p.avg_eth_usd_price, 0) as eth_usd_cover_total,
    --DAI
    coalesce(ac.dai_cover_total * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_cover_total,
    coalesce(ac.dai_cover_total * p.avg_dai_usd_price, 0) as dai_usd_cover_total,
    --USDC
    coalesce(ac.usdc_cover_total * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_cover_total,
    coalesce(ac.usdc_cover_total * p.avg_usdc_usd_price, 0) as usdc_usd_cover_total,
    --== fees ==
    --ETH
    coalesce(ac.eth_premium_total, 0) as eth_eth_premium_total,
    coalesce(ac.eth_premium_total * p.avg_eth_usd_price, 0) as eth_usd_premium_total,
    --DAI
    coalesce(ac.dai_premium_total * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_premium_total,
    coalesce(ac.dai_premium_total * p.avg_dai_usd_price, 0) as dai_usd_premium_total,
    --NXM
    coalesce(ac.nxm_premium_total * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_premium_total,
    coalesce(ac.nxm_premium_total * p.avg_nxm_usd_price, 0) as nxm_usd_premium_total
  from daily_active_covers ac
    inner join daily_avg_prices p on ac.block_date = p.block_date
)

select
  *
from daily_active_covers_enriched
order by 1 desc
