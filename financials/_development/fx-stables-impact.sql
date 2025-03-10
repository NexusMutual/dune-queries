with

prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_usdc_usd_price
  from query_4627588 -- Capital Pool - base root
),

capital_pool as (
  select
    date_trunc('month', block_date) as block_month,
    debt_usdc,
    lag(debt_usdc, 1) over (order by block_date) as debt_usdc_prev,
    dai,
    lag(dai, 1) over (order by block_date) as dai_prev,
    usdc,
    lag(usdc, 1) over (order by block_date) as usdc_prev,
    cover_re,
    lag(cover_re, 1) over (order by block_date) as cover_re_prev
  from (
    select
      block_date,
      avg_capital_pool_eth_total as capital_pool,
      avg_aave_debt_usdc_eth_total as debt_usdc,
      avg_dai_eth_total as dai,
      avg_usdc_eth_total as usdc,
      avg_cover_re_usdc_eth_total as cover_re,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from query_4627588 -- Capital Pool - base root
  ) t
  where t.rn = 1
  order by 1 desc
  limit 12 -- 12 months rolling
),

/*
-- start: 1st of the month
-- end: last of the month
prices_start_end as (
  select distinct
    date_trunc('month', block_date) as block_month,
    first_value(avg_eth_usd_price) over (partition by date_trunc('month', block_date) order by block_date asc) as eth_usd_price_start,
    first_value(avg_eth_usd_price) over (partition by date_trunc('month', block_date) order by block_date desc) as eth_usd_price_end
  from prices
),
*/

-- start: last of the previous month
-- end: last of the month
prices_start_end as (
  select
    date_trunc('month', block_date) as block_month,
    lag(avg_eth_usd_price, 1) over (order by block_date) as eth_usd_price_start,
    avg_eth_usd_price as eth_usd_price_end
  from (
    select
      block_date,
      avg_eth_usd_price,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from prices
  ) t
  where rn = 1
),

stables_fx_impact as (
  select
    s.block_month,
    p.eth_usd_price_start,
    p.eth_usd_price_end,
    -- stables expressed in ETH
    s.dai_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.dai_prev as dai_fx_change,
    s.usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.usdc_prev as usdc_fx_change,
    s.cover_re_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.cover_re_prev as cover_re_fx_change,
    s.debt_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.debt_usdc_prev as debt_usdc_fx_change
  from capital_pool s
    inner join prices_start_end p on s.block_month = p.block_month
)

select
  block_month,
  eth_usd_price_start,
  eth_usd_price_end,
  dai_fx_change,
  usdc_fx_change,
  cover_re_fx_change,
  debt_usdc_fx_change,
  dai_fx_change + usdc_fx_change + cover_re_fx_change + debt_usdc_fx_change as fx_change
from stables_fx_impact
order by 1 desc
