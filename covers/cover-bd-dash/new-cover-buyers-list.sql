with

daily_avg_prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_dai_usd_price,
    avg_usdc_usd_price,
    avg_cbbtc_usd_price,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
  where block_date >= timestamp '2023-03-16' -- since v2
),

covers as (
  select distinct
    cover_id,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured,
    cover_owner,
    if(cover_asset = 'ETH', sum_assured, 0) as eth_cover_amount,
    if(cover_asset = 'DAI', sum_assured, 0) as dai_cover_amount,
    if(cover_asset = 'USDC', sum_assured, 0) as usdc_cover_amount,
    if(cover_asset = 'cbBTC', sum_assured, 0) as cbbtc_cover_amount
  from query_4599092 -- covers v2 - base root (fallback query)
  --where is_migrated = false
  where block_date >= timestamp '2023-03-16'
    -- exclude syndicates / partners
    and cover_owner not in (
      0xe4994082a0e7f38b565e6c5f4afd608de5eddfbb, -- OC
      0x40329f3e27dd3fe228799b4a665f6f104c2ab6b4, -- OC
      0x5f2b6e70aa6a217e9ecd1ed7d0f8f38ce9a348a2, -- OC
      0x8b86cf2684a3af9dd34defc62a18a96deadc40ff, -- TRM
      0x666b8ebfbf4d5f0ce56962a25635cff563f13161, -- Sherlock
      0x5b453a19845e7492ee3a0df4ef085d4c75e5752b, -- Liquid Collective
      0x2557fe0959934f3814c6ee72ab46e6687b81b8ca  -- Ensuro
    )
),

cover_base as (
  select
    c.cover_owner,
    c.cover_id,
    c.cover_start_date,
    c.cover_end_date,
    --ETH
    c.eth_cover_amount,
    c.eth_cover_amount * p.avg_eth_usd_price as eth_usd_cover_amount,
    --DAI
    c.dai_cover_amount * p.avg_dai_usd_price / p.avg_eth_usd_price as dai_eth_cover_amount,
    c.dai_cover_amount * p.avg_dai_usd_price as dai_usd_cover_amount,
    --USDC
    c.usdc_cover_amount * p.avg_usdc_usd_price / p.avg_eth_usd_price as usdc_eth_cover_amount,
    c.usdc_cover_amount * p.avg_usdc_usd_price as usdc_usd_cover_amount,
    --cbBTC
    c.cbbtc_cover_amount * p.avg_cbbtc_usd_price / p.avg_eth_usd_price as cbbtc_eth_cover_amount,
    c.cbbtc_cover_amount * p.avg_cbbtc_usd_price as cbbtc_usd_cover_amount
  from covers c
    inner join daily_avg_prices p on c.cover_start_date = p.block_date
),

cover_totals as (
  select
    cover_owner,
    cover_id,
    cover_start_date,
    cover_end_date,
    eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount as usd_cover_amount,
    eth_cover_amount + dai_eth_cover_amount + usdc_eth_cover_amount + cbbtc_eth_cover_amount as eth_cover_amount,
    row_number() over (partition by cover_owner order by cover_start_date) as first_seen
  from cover_base
)

select
  cover_owner,
  cover_id,
  cover_start_date,
  cover_end_date,
  usd_cover_amount,
  eth_cover_amount
from cover_totals
where first_seen = 1
order by 1, 2
