with

prices as (
  select distinct
    date_trunc('month', block_date) as block_month,
    first_value(avg_eth_usd_price) over (partition by date_trunc('month', block_date) order by block_date asc) as eth_usd_price_start,
    first_value(avg_eth_usd_price) over (partition by date_trunc('month', block_date) order by block_date desc) as eth_usd_price_end
  from nexusmutual_ethereum.capital_pool_totals
),

stables as (
  select
    date_trunc('month', block_date) as block_month,
    avg_dai_eth_total,
    avg_usdc_eth_total,
    avg_cover_re_usdc_eth_total,
    avg_aave_debt_usdc_eth_total
  from nexusmutual_ethereum.capital_pool_totals
  where day(block_date) = 1
),

fx_impact as (
  select
    date_format(s.block_month, '%Y-%m') as block_month,
    p.eth_usd_price_start,
    p.eth_usd_price_end,
    s.avg_dai_eth_total * p.eth_usd_price_start / p.eth_usd_price_end - s.avg_dai_eth_total as dai_fx_change,
    s.avg_usdc_eth_total * p.eth_usd_price_start / p.eth_usd_price_end - s.avg_usdc_eth_total as usdc_fx_change,
    s.avg_cover_re_usdc_eth_total * p.eth_usd_price_start / p.eth_usd_price_end - s.avg_cover_re_usdc_eth_total as cover_re_fx_change,
    s.avg_aave_debt_usdc_eth_total * p.eth_usd_price_start / p.eth_usd_price_end - s.avg_aave_debt_usdc_eth_total as aave_debt_usdc_fx_change
  from stables s
    inner join prices p on s.block_month = p.block_month
)

select
  block_month,
  p.eth_usd_price_start,
  p.eth_usd_price_end,
  dai_fx_change,
  usdc_fx_change,
  cover_re_fx_change,
  aave_debt_usdc_fx_change,
  dai_fx_change + usdc_fx_change + cover_re_fx_change + aave_debt_usdc_fx_change as fx_change
from fx_impact
order by 1 desc
