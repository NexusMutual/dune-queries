with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
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
    inner join nexusmutual_ethereum.covers_v2 c
    --inner join query_3788370 c -- covers v2 base (fallback) query
      on p.block_date = c.block_date
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
  case
    when c.commission_destination in (
      0x586b9b2f8010b284a0197f392156f1a7eb5e86e9,
      0x8e53D04644E9ab0412a8c6bd228C84da7664cFE3
    ) then 'Community Fund'
    when c.commission_destination = 0x95abc2a62ee543217cf7640b277ba13d056d904a then 'Unity'
    when c.commission_destination = 0xac0734c62b316041d190438d5d3e5d1359614407 then 'Bright Union'
    when c.commission_destination in (
      0xe4994082a0e7f38b565e6c5f4afd608de5eddfbb,
      0x40329f3e27dd3fe228799b4a665f6f104c2ab6b4,
      0x5f2b6e70aa6a217e9ecd1ed7d0f8f38ce9a348a2
    ) then 'OpenCover'
    else coalesce(ens.name, cast(c.commission_destination as varchar))
  end as commission_destination,
  c.eth_commission,
  c.usd_commission
from commissions_agg c
  left join labels.ens on c.commission_destination = ens.address
order by 1, 2
