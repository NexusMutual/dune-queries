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
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    c.staking_pool,
    c.product_type,
    c.product_name,
    /*
    c.cover_asset,
    c.cover_amount * case c.cover_asset
        when 'ETH' then 1
        when 'DAI' then p.avg_dai_usd_price / p.avg_eth_usd_price
        when 'USDC' then p.avg_usdc_usd_price / p.avg_eth_usd_price
      end as eth_cover_amount,
    c.cover_amount * case c.cover_asset
        when 'ETH' then p.avg_eth_usd_price
        when 'DAI' then p.avg_dai_usd_price
        when 'USDC' then p.avg_usdc_usd_price
      end as usd_cover_amount,
    c.premium_asset,
    c.premium * case when c.staking_pool = 'v1'
        then if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price) / p.avg_eth_usd_price
        else p.avg_nxm_usd_price / p.avg_eth_usd_price
      end eth_premium_amount,
    c.premium * case when c.staking_pool = 'v1'
        then if(c.cover_asset = 'DAI', p.avg_dai_usd_price, p.avg_eth_usd_price)
        else p.avg_nxm_usd_price
      end usd_premium_amount,
    */
    c.cover_asset,
    if(c.cover_asset = 'ETH', c.cover_amount, 0) as eth_cover_amount,
    if(c.cover_asset = 'DAI', c.cover_amount, 0) as dai_cover_amount,
    if(c.cover_asset = 'USDC', c.cover_amount, 0) as usdc_cover_amount,
    c.premium_asset,
    if(c.staking_pool = 'v1' and c.cover_asset = 'ETH', c.premium, 0) as eth_premium_amount,
    if(c.staking_pool = 'v1' and c.cover_asset = 'DAI', c.premium, 0) as dai_premium_amount,
    if(c.staking_pool <> 'v1', c.premium, 0) as nxm_premium_amount
  from covers c
    --inner join daily_avg_prices p on c.cover_start_date = p.block_date
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2019-07-12', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_active_covers as (
  select
    ds.block_date,
    sum(c.eth_cover_amount) as eth_cover_total,
    sum(c.dai_cover_amount) as dai_cover_total,
    sum(c.usdc_cover_amount) as usdc_cover_total,
    sum(c.eth_premium_amount) as eth_premium_total,
    sum(c.dai_premium_amount) as dai_premium_total,
    sum(c.nxm_premium_amount) as nxm_premium_total
  from day_sequence ds
    left join covers_ext c on ds.block_date between c.cover_start_date and c.cover_end_date
  group by 1
),

daily_active_covers_enriched as (
  select
    ac.block_date,
    --== covers ==
    --ETH
    ac.eth_cover_total as eth_eth_cover_total,
    ac.eth_cover_total * p.avg_eth_usd_price as eth_usd_cover_total,
    --DAI
    ac.dai_cover_total * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_cover_total,
    ac.dai_cover_total * p.avg_dai_usd_price as dai_usd_cover_total,
    --USDC
    ac.usdc_cover_total * p.avg_usdc_usd_price / p.avg_eth_usd_price as usdc_eth_cover_total,
    ac.usdc_cover_total * p.avg_usdc_usd_price as usdc_usd_cover_total,
    --== fees ==
    --ETH
    ac.eth_premium_total as eth_eth_premium_total,
    ac.eth_premium_total * p.avg_eth_usd_price as eth_usd_premium_total,
    --DAI
    ac.dai_premium_total * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_premium_total,
    ac.dai_premium_total * p.avg_dai_usd_price as dai_usd_premium_total,
    --NXM
    ac.nxm_premium_total * p.avg_nxm_usd_price / p.avg_eth_usd_price as nxm_eth_premium_total,
    ac.nxm_premium_total * p.avg_nxm_usd_price as nxm_usd_premium_total
  from daily_active_covers ac
    inner join daily_avg_prices p on ac.block_date = p.block_date
)

select
  *
from daily_active_covers_enriched
order by 1 desc
