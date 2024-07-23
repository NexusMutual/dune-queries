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

commissions_ext as (
  select
    date_trunc('month', c.cover_start_date) as cover_month,
    c.commission,
    c.commission_destination,
    --ETH
    if(c.premium_asset = 'ETH', c.commission * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as eth_eth_commission,
    if(c.premium_asset = 'ETH', c.commission * p.avg_nxm_usd_price, 0) as eth_usd_commission,
    --DAI
    if(c.premium_asset = 'DAI', c.commission * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as dai_eth_commission,
    if(c.premium_asset = 'DAI', c.commission * p.avg_nxm_usd_price, 0) as dai_usd_commission,
    --USDC
    if(c.premium_asset = 'USDC', c.commission * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as usdc_eth_commission,
    if(c.premium_asset = 'USDC', c.commission * p.avg_nxm_usd_price, 0) as usdc_usd_commission,
    --NXM
    if(c.premium_asset = 'NXM', c.commission * p.avg_nxm_usd_price / p.avg_eth_usd_price, 0) as nxm_eth_commission,
    if(c.premium_asset = 'NXM', c.commission * p.avg_nxm_usd_price, 0) as nxm_usd_commission
  from daily_avg_prices p
    inner join query_3788370 c on p.block_date = c.block_date
  where c.commission > 0
),

commissions_agg as (
  select
    cover_month,
    commission_destination,
    sum(eth_eth_commission + dai_eth_commission + usdc_eth_commission + nxm_eth_commission) as eth_commission,
    sum(eth_usd_commission + dai_usd_commission + usdc_usd_commission + nxm_usd_commission) as usd_commission
  from commissions_ext
  group by 1, 2
)

select
  c.cover_month,
  case c.commission_destination
    when 0x586b9b2f8010b284a0197f392156f1a7eb5e86e9 then 'Community Fund'
    when 0x95abc2a62ee543217cf7640b277ba13d056d904a then 'Unity'
    when 0xac0734c62b316041d190438d5d3e5d1359614407 then 'Bright Union'
    else coalesce(ens.name, cast(c.commission_destination as varchar))
  end as commission_destination,
  c.eth_commission,
  c.usd_commission
from commissions_agg c
  left join labels.ens on c.commission_destination = ens.address
order by 1, 2
