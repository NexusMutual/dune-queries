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
    date_diff('day', cover_start_date, cover_end_date) as cover_period,
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

daily_active_cover as (
  select
    ds.block_date,
    c_period.cover_id,
    c_period.cover_period,
    --== cover ==
    --ETH
    coalesce(c_period.eth_cover_amount, 0) as eth_eth_active_cover,
    coalesce(c_period.eth_cover_amount * p.avg_eth_usd_price, 0) as eth_usd_active_cover,
    --DAI
    coalesce(c_period.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_active_cover,
    coalesce(c_period.dai_cover_amount * p.avg_dai_usd_price, 0) as dai_usd_active_cover,
    --USDC
    coalesce(c_period.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_active_cover,
    coalesce(c_period.usdc_cover_amount * p.avg_usdc_usd_price, 0) as usdc_usd_active_cover,
    --== fees ==
    --ETH
    coalesce(c_period.eth_premium_amount, 0) as eth_eth_active_premium,
    coalesce(c_period.eth_premium_amount * p.avg_eth_usd_price, 0) as eth_usd_active_premium,
    --DAI
    coalesce(c_period.dai_premium_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_active_premium,
    coalesce(c_period.dai_premium_amount * p.avg_dai_usd_price, 0) as dai_usd_active_premium,
    --NXM
    coalesce(c_period.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_active_premium,
    coalesce(c_period.nxm_premium_amount * p.avg_nxm_usd_price, 0) as nxm_usd_active_premium
  from day_sequence ds
    left join covers_ext c_period on ds.block_date between c_period.cover_start_date and c_period.cover_end_date
    left join daily_avg_prices p on ds.block_date = p.block_date
),

daily_cover_sales as (
  select
    ds.block_date,
    c_start.cover_id,
    c_start.cover_period,
    --== cover ==
    --ETH
    coalesce(c_start.eth_cover_amount, 0) as eth_eth_cover,
    coalesce(c_start.eth_cover_amount * p.avg_eth_usd_price, 0) as eth_usd_cover,
    --DAI
    coalesce(c_start.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_cover,
    coalesce(c_start.dai_cover_amount * p.avg_dai_usd_price, 0) as dai_usd_cover,
    --USDC
    coalesce(c_start.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_cover,
    coalesce(c_start.usdc_cover_amount * p.avg_usdc_usd_price, 0) as usdc_usd_cover,
    --== fees ==
    --ETH
    coalesce(c_start.eth_premium_amount, 0) as eth_eth_premium,
    coalesce(c_start.eth_premium_amount * p.avg_eth_usd_price, 0) as eth_usd_premium,
    --DAI
    coalesce(c_start.dai_premium_amount * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_premium,
    coalesce(c_start.dai_premium_amount * p.avg_dai_usd_price, 0) as dai_usd_premium,
    --NXM
    coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_premium,
    coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price, 0) as nxm_usd_premium
  from day_sequence ds
    left join covers_ext c_start on ds.block_date = c_start.cover_start_date
    left join daily_avg_prices p on ds.block_date = p.block_date
),

daily_active_cover_aggs as (
  select
    block_date,
    count(distinct cover_id) as active_cover,
    --== cover ==
    sum(eth_eth_active_cover) as eth_eth_active_cover,
    sum(dai_eth_active_cover) as dai_eth_active_cover,
    sum(usdc_eth_active_cover) as usdc_eth_active_cover,
    sum(eth_eth_active_cover) + sum(dai_eth_active_cover) + sum(usdc_eth_active_cover) as eth_active_cover,
    approx_percentile(eth_eth_active_cover + dai_eth_active_cover + usdc_eth_active_cover, 0.5) as median_eth_active_cover,
    sum(eth_usd_active_cover) as eth_usd_active_cover,
    sum(dai_usd_active_cover) as dai_usd_active_cover,
    sum(usdc_usd_active_cover) as usdc_usd_active_cover,
    sum(eth_usd_active_cover) + sum(dai_usd_active_cover) + sum(usdc_usd_active_cover) as usd_active_cover,
    approx_percentile(eth_usd_active_cover + dai_usd_active_cover + usdc_usd_active_cover, 0.5) as median_usd_active_cover,
    --== fees ==
    sum(eth_eth_active_premium * 365 / cover_period) as eth_eth_active_premium,
    sum(dai_eth_active_premium * 365 / cover_period) as dai_eth_active_premium,
    sum(nxm_eth_active_premium * 365 / cover_period) as nxm_eth_active_premium,
    sum(eth_eth_active_premium * 365 / cover_period)
      + sum(dai_eth_active_premium * 365 / cover_period)
      + sum(nxm_eth_active_premium * 365 / cover_period) as eth_active_premium,
    approx_percentile(
      (eth_eth_active_premium * 365 / cover_period)
      + (dai_eth_active_premium * 365 / cover_period)
      + (nxm_eth_active_premium * 365 / cover_period), 0.5) as median_eth_active_premium,
    sum(eth_usd_active_premium * 365 / cover_period) as eth_usd_active_premium,
    sum(dai_usd_active_premium * 365 / cover_period) as dai_usd_active_premium,
    sum(nxm_usd_active_premium * 365 / cover_period) as nxm_usd_active_premium,
    sum(eth_usd_active_premium * 365 / cover_period)
      + sum(dai_usd_active_premium * 365 / cover_period)
      + sum(nxm_usd_active_premium * 365 / cover_period) as usd_active_premium,
    approx_percentile(
      (eth_usd_active_premium * 365 / cover_period)
      + (dai_usd_active_premium * 365 / cover_period)
      + (nxm_usd_active_premium * 365 / cover_period), 0.5) as median_usd_active_premium
  from daily_active_cover
  group by 1
),

daily_cover_sales_aggs as (
  select
    block_date,
    count(distinct cover_id) as cover_sold,
    --== cover ==
    sum(eth_eth_cover) as eth_eth_cover,
    sum(dai_eth_cover) as dai_eth_cover,
    sum(usdc_eth_cover) as usdc_eth_cover,
    sum(eth_eth_cover) + sum(dai_eth_cover) + sum(usdc_eth_cover) as eth_cover,
    approx_percentile(eth_eth_cover + dai_eth_cover + usdc_eth_cover, 0.5) as median_eth_cover,
    sum(eth_usd_cover) as eth_usd_cover,
    sum(dai_usd_cover) as dai_usd_cover,
    sum(usdc_usd_cover) as usdc_usd_cover,
    sum(eth_usd_cover) + sum(dai_usd_cover) + sum(usdc_usd_cover) as usd_cover,
    approx_percentile(eth_usd_cover + dai_usd_cover + usdc_usd_cover, 0.5) as median_usd_cover,
    --== fees ==
    sum(eth_eth_premium) as eth_eth_premium,
    sum(dai_eth_premium) as dai_eth_premium,
    sum(nxm_eth_premium) as nxm_eth_premium,
    sum(eth_eth_premium) + sum(dai_eth_premium) + sum(nxm_eth_premium) as eth_premium,
    approx_percentile(eth_eth_premium + dai_eth_premium + nxm_eth_premium, 0.5) as median_eth_premium,
    sum(eth_usd_premium) as eth_usd_premium,
    sum(dai_usd_premium) as dai_usd_premium,
    sum(nxm_usd_premium) as nxm_usd_premium,
    sum(eth_usd_premium) + sum(dai_usd_premium) + sum(nxm_usd_premium) as usd_premium,
    approx_percentile(eth_usd_premium + dai_usd_premium + nxm_usd_premium, 0.5) as median_usd_premium
  from daily_cover_sales
  group by 1
)

select
  ac.block_date,
  --**** ACTIVE COVER ****
  ac.active_cover,
  ac.eth_eth_active_cover,
  ac.dai_eth_active_cover,
  ac.usdc_eth_active_cover,
  ac.eth_active_cover,
  ac.median_eth_active_cover,
  ac.eth_usd_active_cover,
  ac.dai_usd_active_cover,
  ac.usdc_usd_active_cover,
  ac.usd_active_cover,
  ac.median_usd_active_cover,
  ac.eth_eth_active_premium,
  ac.dai_eth_active_premium,
  ac.nxm_eth_active_premium,
  ac.eth_active_premium,
  ac.median_eth_active_premium,
  ac.eth_usd_active_premium,
  ac.dai_usd_active_premium,
  ac.nxm_usd_active_premium,
  ac.usd_active_premium,
  ac.median_usd_active_premium,
  --**** COVER SALES ****
  cs.cover_sold,
  cs.eth_eth_cover,
  cs.dai_eth_cover,
  cs.usdc_eth_cover,
  cs.eth_cover,
  cs.median_eth_cover,
  cs.eth_usd_cover,
  cs.dai_usd_cover,
  cs.usdc_usd_cover,
  cs.usd_cover,
  cs.median_usd_cover,
  cs.eth_eth_premium,
  cs.dai_eth_premium,
  cs.nxm_eth_premium,
  cs.eth_premium,
  cs.median_eth_premium,
  cs.eth_usd_premium,
  cs.dai_usd_premium,
  cs.nxm_usd_premium,
  cs.usd_premium,
  cs.median_usd_premium
from daily_active_cover_aggs ac
  inner join daily_cover_sales_aggs cs on ac.block_date = cs.block_date
--order by 1 desc
