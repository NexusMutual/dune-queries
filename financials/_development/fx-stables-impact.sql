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
    eth_debt_usdc,
    lag(eth_debt_usdc, 1) over (order by block_date) as eth_debt_usdc_prev,
    eth_dai,
    lag(eth_dai, 1) over (order by block_date) as eth_dai_prev,
    eth_usdc,
    lag(eth_usdc, 1) over (order by block_date) as eth_usdc_prev,
    eth_cover_re,
    lag(eth_cover_re, 1) over (order by block_date) as eth_cover_re_prev,
    usd_debt_usdc,
    lag(usd_debt_usdc, 1) over (order by block_date) as usd_debt_usdc_prev,
    usd_dai,
    lag(usd_dai, 1) over (order by block_date) as usd_dai_prev,
    usd_usdc,
    lag(usd_usdc, 1) over (order by block_date) as usd_usdc_prev,
    usd_cover_re,
    lag(usd_cover_re, 1) over (order by block_date) as usd_cover_re_prev
  from (
    select
      block_date,
      avg_aave_debt_usdc_eth_total as eth_debt_usdc,
      avg_dai_eth_total as eth_dai,
      avg_usdc_eth_total as eth_usdc,
      avg_cover_re_usdc_eth_total as eth_cover_re,
      avg_aave_debt_usdc_usd_total as usd_debt_usdc,
      avg_dai_usd_total as usd_dai,
      avg_usdc_usd_total as usd_usdc,
      avg_cover_re_usdc_usd_total as usd_cover_re,
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
    s.eth_dai_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_dai_prev as eth_dai_fx_change,
    s.eth_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_usdc_prev as eth_usdc_fx_change,
    s.eth_cover_re_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_cover_re_prev as eth_cover_re_fx_change,
    s.eth_debt_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_debt_usdc_prev as eth_debt_usdc_fx_change,
    -- stables expressed in USD
    (s.eth_dai_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_dai_prev) * p.eth_usd_price_end as usd_dai_fx_change,
    (s.eth_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_usdc_prev) * p.eth_usd_price_end as usd_usdc_fx_change,
    (s.eth_cover_re_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_cover_re_prev) * p.eth_usd_price_end as usd_cover_re_fx_change,
    (s.eth_debt_usdc_prev * p.eth_usd_price_start / p.eth_usd_price_end - s.eth_debt_usdc_prev) * p.eth_usd_price_end as usd_debt_usdc_fx_change
  from capital_pool s
    inner join prices_start_end p on s.block_month = p.block_month
)

select
  block_month,
  eth_usd_price_start,
  eth_usd_price_end,
  /*eth_dai_fx_change,
  eth_usdc_fx_change,
  eth_cover_re_fx_change,
  eth_debt_usdc_fx_change,*/
  eth_dai_fx_change + eth_usdc_fx_change + eth_cover_re_fx_change + eth_debt_usdc_fx_change as eth_fx_change,
  /*usd_dai_fx_change,
  usd_usdc_fx_change,
  usd_cover_re_fx_change,
  usd_debt_usdc_fx_change,*/
  usd_dai_fx_change + usd_usdc_fx_change + usd_cover_re_fx_change + usd_debt_usdc_fx_change as usd_fx_change
from stables_fx_impact
order by 1 desc
