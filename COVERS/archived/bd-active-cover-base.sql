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
  --from nexusmutual_ethereum.capital_pool_prices
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

daily_active_cover as (
  select
    ds.block_date,
    count(distinct c_period.cover_id) as active_cover_sold,
    sum(c_period.eth_cover_amount) as eth_active_cover_total,
    sum(c_period.dai_cover_amount) as dai_active_cover_total,
    sum(c_period.usdc_cover_amount) as usdc_active_cover_total,
    sum(c_period.eth_premium_amount) as eth_active_premium_total,
    sum(c_period.dai_premium_amount) as dai_active_premium_total,
    sum(c_period.nxm_premium_amount) as nxm_active_premium_total
  from day_sequence ds
    left join covers_ext c_period on ds.block_date between c_period.cover_start_date and c_period.cover_end_date
  group by 1
),

daily_cover_sales as (
  select
    ds.block_date,
    count(distinct c_start.cover_id) as cover_sold,
    sum(c_start.eth_cover_amount) as eth_cover_total,
    sum(c_start.dai_cover_amount) as dai_cover_total,
    sum(c_start.usdc_cover_amount) as usdc_cover_total,
    sum(c_start.eth_premium_amount) as eth_premium_total,
    sum(c_start.dai_premium_amount) as dai_premium_total,
    sum(c_start.nxm_premium_amount) as nxm_premium_total
  from day_sequence ds
    left join covers_ext c_start on ds.block_date = c_start.cover_start_date
  group by 1
),

daily_cover_enriched as (
  select
    ac.block_date,
    --**** ACTIVE COVER ****
    ac.active_cover_sold,
    --== cover ==
    --ETH
    coalesce(ac.eth_active_cover_total, 0) as eth_eth_active_cover,
    coalesce(ac.eth_active_cover_total * p.avg_eth_usd_price, 0) as eth_usd_active_cover,
    --DAI
    coalesce(ac.dai_active_cover_total * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_active_cover,
    coalesce(ac.dai_active_cover_total * p.avg_dai_usd_price, 0) as dai_usd_active_cover,
    --USDC
    coalesce(ac.usdc_active_cover_total * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_active_cover,
    coalesce(ac.usdc_active_cover_total * p.avg_usdc_usd_price, 0) as usdc_usd_active_cover,
    --== fees ==
    --ETH
    coalesce(ac.eth_active_premium_total, 0) as eth_eth_active_premium,
    coalesce(ac.eth_active_premium_total * p.avg_eth_usd_price, 0) as eth_usd_active_premium,
    --DAI
    coalesce(ac.dai_active_premium_total * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_active_premium,
    coalesce(ac.dai_active_premium_total * p.avg_dai_usd_price, 0) as dai_usd_active_premium,
    --NXM
    coalesce(ac.nxm_active_premium_total * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_active_premium,
    coalesce(ac.nxm_active_premium_total * p.avg_nxm_usd_price, 0) as nxm_usd_active_premium,
    --**** COVER SALES ****
    cs.cover_sold,
    --== cover ==
    --ETH
    coalesce(cs.eth_cover_total, 0) as eth_eth_cover,
    coalesce(cs.eth_cover_total * p.avg_eth_usd_price, 0) as eth_usd_cover,
    --DAI
    coalesce(cs.dai_cover_total * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_cover,
    coalesce(cs.dai_cover_total * p.avg_dai_usd_price, 0) as dai_usd_cover,
    --USDC
    coalesce(cs.usdc_cover_total * p.avg_usdc_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_cover,
    coalesce(cs.usdc_cover_total * p.avg_usdc_usd_price, 0) as usdc_usd_cover,
    --== fees ==
    --ETH
    coalesce(cs.eth_premium_total, 0) as eth_eth_premium,
    coalesce(cs.eth_premium_total * p.avg_eth_usd_price, 0) as eth_usd_premium,
    --DAI
    coalesce(cs.dai_premium_total * p.avg_dai_usd_price / p.avg_eth_usd_price, 0) as dai_eth_premium,
    coalesce(cs.dai_premium_total * p.avg_dai_usd_price, 0) as dai_usd_premium,
    --NXM
    coalesce(cs.nxm_premium_total * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_premium,
    coalesce(cs.nxm_premium_total * p.avg_nxm_usd_price, 0) as nxm_usd_premium
  from daily_active_cover ac
    inner join daily_cover_sales cs on ac.block_date = cs.block_date
    inner join daily_avg_prices p on ac.block_date = p.block_date
)

select
  block_date,
  --**** ACTIVE COVER ****
  active_cover_sold,
  eth_eth_active_cover + dai_eth_active_cover + usdc_eth_active_cover as eth_active_cover,
  eth_eth_active_cover,
  dai_eth_active_cover,
  usdc_eth_active_cover,
  eth_usd_active_cover + dai_usd_active_cover + usdc_usd_active_cover as usd_active_cover,
  eth_usd_active_cover,
  dai_usd_active_cover,
  usdc_usd_active_cover,
  eth_eth_active_premium + dai_eth_active_premium + nxm_eth_active_premium as eth_active_premium,
  eth_eth_active_premium,
  dai_eth_active_premium,
  nxm_eth_active_premium,
  eth_usd_active_premium + dai_usd_active_premium + nxm_usd_active_premium as usd_active_premium,
  eth_usd_active_premium,
  dai_usd_active_premium,
  nxm_usd_active_premium,
  --**** COVER SALES ****
  cover_sold,
  eth_eth_cover + dai_eth_cover + usdc_eth_cover as eth_cover,
  eth_eth_cover,
  dai_eth_cover,
  usdc_eth_cover,
  eth_usd_cover + dai_usd_cover + usdc_usd_cover as usd_cover,
  eth_usd_cover,
  dai_usd_cover,
  usdc_usd_cover,
  eth_eth_premium + dai_eth_premium + nxm_eth_premium as eth_premium,
  eth_eth_premium,
  dai_eth_premium,
  nxm_eth_premium,
  eth_usd_premium + dai_usd_premium + nxm_usd_premium as usd_premium,
  eth_usd_premium,
  dai_usd_premium,
  nxm_usd_premium
from daily_cover_enriched
--order by 1 desc
