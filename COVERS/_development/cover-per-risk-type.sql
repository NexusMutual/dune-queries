with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  from nexusmutual_ethereum.capital_pool_prices
),

covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    case
      when product_type = 'The Retail Mutual' then 'TRM'
      when product_type like '%ETH Staking%' or product_type like 'ETH Slashing%' then 'ETH Staking'
      when product_type = 'OpenCover Transaction' then 'OC Transaction'
      when product_type in ('Protocol', 'Native Protocol', 'Bundled Protocol Cover', 'Fund Portfolio Cover', 'UnoRe Quota Share')
        or product_type like 'Sherlock%' then 'Bundled'
      else product_type
    end as product_type,
    cover_asset,
    sum_assured * partial_cover_amount / sum(partial_cover_amount) over (partition by cover_id) as cover_amount,
    premium_asset,
    premium_incl_commission as premium
  from nexusmutual_ethereum.covers_v2
  where is_migrated = false
),

covers_ext as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    date_diff('day', cover_start_date, cover_end_date) as cover_period,
    product_type,
    cover_asset,
    if(cover_asset = 'ETH', cover_amount, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', cover_amount, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', cover_amount, 0) as usdc_cover_amount,
    premium_asset,
    coalesce(premium, 0) as nxm_premium_amount
  from covers
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(current_date - interval '365' day, current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_active_cover as (
  select
    ds.block_date,
    c_period.cover_id,
    c_period.cover_period,
    c_period.product_type,
    date_diff('day', current_date, cover_end_date) as day_diff,
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
    --== active premium in force ==
    --ETH
    coalesce(if(c_period.premium_asset = 'ETH', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as eth_eth_active_premium,
    coalesce(if(c_period.premium_asset = 'ETH', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)), 0) as eth_usd_active_premium,
    --DAI
    coalesce(if(c_period.premium_asset = 'DAI', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as dai_eth_active_premium,
    coalesce(if(c_period.premium_asset = 'DAI', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)), 0) as dai_usd_active_premium,
    --USDC
    coalesce(if(c_period.premium_asset = 'USDC', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as usdc_eth_active_premium,
    coalesce(if(c_period.premium_asset = 'USDC', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)), 0) as usdc_usd_active_premium,
    --NXM
    coalesce(if(c_period.premium_asset = 'NXM', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as nxm_eth_active_premium,
    coalesce(if(c_period.premium_asset = 'NXM', coalesce(c_period.nxm_premium_amount * 365 / c_period.cover_period * p.avg_nxm_usd_price, 0)), 0) as nxm_usd_active_premium
  from day_sequence ds
    inner join daily_avg_prices p on ds.block_date = p.block_date
    left join covers_ext c_period on ds.block_date between c_period.cover_start_date and c_period.cover_end_date
),

daily_cover_sales as (
  select
    ds.block_date,
    c_start.cover_id,
    c_start.cover_period,
    c_start.product_type,
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
    coalesce(if(c_start.premium_asset = 'ETH', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as eth_eth_premium,
    coalesce(if(c_start.premium_asset = 'ETH', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price, 0)), 0) as eth_usd_premium,
    --DAI
    coalesce(if(c_start.premium_asset = 'DAI', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as dai_eth_premium,
    coalesce(if(c_start.premium_asset = 'DAI', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price, 0)), 0) as dai_usd_premium,
    --USDC
    coalesce(if(c_start.premium_asset = 'USDC', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as usdc_eth_premium,
    coalesce(if(c_start.premium_asset = 'USDC', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price, 0)), 0) as usdc_usd_premium,
    --NXM
    coalesce(if(c_start.premium_asset = 'NXM', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0)), 0) as nxm_eth_premium,
    coalesce(if(c_start.premium_asset = 'NXM', coalesce(c_start.nxm_premium_amount * p.avg_nxm_usd_price, 0)), 0) as nxm_usd_premium
  from day_sequence ds
    inner join daily_avg_prices p on ds.block_date = p.block_date
    left join covers_ext c_start on ds.block_date = c_start.cover_start_date
),

daily_active_cover_aggs as (
  select
    block_date,
    product_type,
    count(distinct cover_id) as active_cover,
    sum(day_diff * (eth_usd_active_cover + dai_usd_active_cover + usdc_usd_active_cover)) / sum(eth_usd_active_cover + dai_usd_active_cover + usdc_usd_active_cover) as wavg_time_to_expiry,
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
    sum(eth_eth_active_premium) as eth_eth_active_premium,
    sum(dai_eth_active_premium) as dai_eth_active_premium,
    sum(usdc_eth_active_premium) as usdc_eth_active_premium,
    sum(nxm_eth_active_premium) as nxm_eth_active_premium,
    sum(eth_eth_active_premium) + sum(dai_eth_active_premium) + sum(usdc_eth_active_premium) + sum(nxm_eth_active_premium) as eth_active_premium,
    approx_percentile(eth_eth_active_premium + dai_eth_active_premium + usdc_eth_active_premium + nxm_eth_active_premium, 0.5) as median_eth_active_premium,
    sum(eth_usd_active_premium) as eth_usd_active_premium,
    sum(dai_usd_active_premium) as dai_usd_active_premium,
    sum(usdc_usd_active_premium) as usdc_usd_active_premium,
    sum(nxm_usd_active_premium) as nxm_usd_active_premium,
    sum(eth_usd_active_premium) + sum(dai_usd_active_premium) + sum(usdc_usd_active_premium) + sum(nxm_usd_active_premium) as usd_active_premium,
    approx_percentile(eth_usd_active_premium + dai_usd_active_premium + usdc_usd_active_premium + nxm_usd_active_premium, 0.5) as median_usd_active_premium
  from daily_active_cover
  group by 1, 2
),

daily_cover_sales_aggs as (
  select
    block_date,
    product_type,
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
    sum(usdc_eth_premium) as usdc_eth_premium,
    sum(nxm_eth_premium) as nxm_eth_premium,
    sum(eth_eth_premium) + sum(dai_eth_premium) + sum(usdc_eth_premium) + sum(nxm_eth_premium) as eth_premium,
    approx_percentile(eth_eth_premium + dai_eth_premium + usdc_eth_premium + nxm_eth_premium, 0.5) as median_eth_premium,
    sum(eth_usd_premium) as eth_usd_premium,
    sum(dai_usd_premium) as dai_usd_premium,
    sum(usdc_usd_premium) as usdc_usd_premium,
    sum(nxm_usd_premium) as nxm_usd_premium,
    sum(eth_usd_premium) + sum(dai_usd_premium) + sum(usdc_usd_premium) + sum(nxm_usd_premium) as usd_premium,
    approx_percentile(eth_usd_premium + dai_usd_premium + usdc_usd_premium + nxm_usd_premium, 0.5) as median_usd_premium
  from daily_cover_sales
  group by 1, 2
),

daily_cover_aggs_combined as (
  select
    ac.block_date,
    ac.product_type,
    row_number() over (partition by ac.product_type order by ac.block_date desc) as rn,
    --**** ACTIVE COVER ****
    ac.wavg_time_to_expiry,
    ac.eth_active_cover,
    ac.usd_active_cover,
    ac.eth_active_premium,
    ac.usd_active_premium,
    --**** COVER SALES ****
    sum(cs.eth_cover) over (order by cs.block_date) as eth_cover,
    sum(cs.usd_cover) over (order by cs.block_date) as usd_cover,
    sum(cs.eth_premium) over (order by cs.block_date) as eth_premium,
    sum(cs.usd_premium) over (order by cs.block_date) as usd_premium
  from daily_active_cover_aggs ac
    inner join daily_cover_sales_aggs cs on ac.block_date = cs.block_date and ac.product_type = cs.product_type
)

select
  block_date,
  product_type,
  --**** ACTIVE COVER ****
  wavg_time_to_expiry,
  eth_active_cover,
  usd_active_cover,
  eth_active_premium,
  usd_active_premium,
  --**** COVER SALES ****
  eth_cover,
  usd_cover,
  eth_premium,
  usd_premium
from daily_cover_aggs_combined
where rn = 1
