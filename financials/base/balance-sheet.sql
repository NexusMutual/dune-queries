with

prices as (
  select
    block_date,
    avg_eth_usd_price
  from query_4627588 -- Capital Pool - base root
),

capital_pool as (
  select
    date_trunc('month', block_date) as block_month,
    eth_capital_pool,
    lag(eth_capital_pool, 1) over (order by block_date) as eth_capital_pool_prev,
    eth_eth,
    eth_steth,
    eth_reth,
    eth_nxmty,
    eth_aweth,
    eth_debt_usdc,
    eth_dai,
    eth_usdc,
    eth_cbbtc,
    eth_cover_re
  from (
    select
      block_date,
      avg_capital_pool_eth_total as eth_capital_pool,
      eth_total as eth_eth,
      steth_total as eth_steth,
      avg_reth_eth_total as eth_reth,
      nxmty_eth_total as eth_nxmty,
      aave_collateral_weth_total as eth_aweth,
      avg_aave_debt_usdc_eth_total as eth_debt_usdc,
      avg_dai_eth_total as eth_dai,
      avg_usdc_eth_total as eth_usdc,
      avg_cbbtc_eth_total as eth_cbbtc,
      avg_cover_re_usdc_eth_total as eth_cover_re,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from query_4627588 -- Capital Pool - base root
  ) t
  where t.rn = 1
  --order by 1 desc
  --limit 12 -- 12 months rolling
),

kiln_rewards as (
  select
    block_month,
    eth_kiln_rewards,
    eth_kiln_rewards_prev
  from query_4872828 -- kiln rewards monthly
),

prices_start_end as (
  select
    date_trunc('month', block_date) as block_month,
    lag(avg_eth_usd_price, 1) over (order by block_date) as eth_usd_price_start, -- start: last of the previous month
    avg_eth_usd_price as eth_usd_price_end -- end: last of the month
  from (
    select
      block_date,
      avg_eth_usd_price,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from prices
  ) t
  where rn = 1
)

select
  --date_format(block_month, '%Y-%m') as block_month,
  cp.block_month,
  cp.eth_capital_pool + coalesce(kr.eth_kiln_rewards, 0) as eth_capital_pool,
  cp.eth_capital_pool_prev + coalesce(kr.eth_kiln_rewards_prev, 0) as eth_capital_pool_prev,
  cp.eth_eth,
  cp.eth_steth,
  cp.eth_reth,
  cp.eth_nxmty + coalesce(kr.eth_kiln_rewards, 0) as eth_nxmty,
  cp.eth_aweth,
  cp.eth_debt_usdc,
  cp.eth_dai,
  cp.eth_usdc,
  cp.eth_cbbtc,
  cp.eth_cover_re,
  (cp.eth_capital_pool + coalesce(kr.eth_kiln_rewards, 0)) * p.eth_usd_price_end as usd_capital_pool,
  (cp.eth_capital_pool_prev + coalesce(kr.eth_kiln_rewards_prev, 0)) * p.eth_usd_price_start as usd_capital_pool_prev,
  cp.eth_eth * p.eth_usd_price_end as usd_eth,
  cp.eth_steth * p.eth_usd_price_end as usd_steth,
  cp.eth_reth * p.eth_usd_price_end as usd_reth,
  (cp.eth_nxmty + coalesce(kr.eth_kiln_rewards, 0)) * p.eth_usd_price_end as usd_nxmty,
  cp.eth_aweth * p.eth_usd_price_end as usd_aweth,
  cp.eth_debt_usdc * p.eth_usd_price_end as usd_debt_usdc,
  cp.eth_dai * p.eth_usd_price_end as usd_dai,
  cp.eth_usdc * p.eth_usd_price_end as usd_usdc,
  cp.eth_cbbtc * p.eth_usd_price_end as usd_cbbtc,
  cp.eth_cover_re * p.eth_usd_price_end as usd_cover_re
from capital_pool cp
  inner join prices_start_end p on cp.block_month = p.block_month
  left join kiln_rewards kr on cp.block_month = kr.block_month
order by 1 desc
