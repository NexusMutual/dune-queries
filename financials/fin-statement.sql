with

items as (
  select fi_id, scope, label, label_tab
  from query_4832890 -- fin items
),

investment_returns as (
  select
    block_month,
    eth_inv_returns,
    eth_inv_apy,
    fx_change
  from query_4770697 -- investement returns
  where block_month = '2025-02'
),

cash_surplus (label, eth_val, apy) as (
  select 'Investment Returns', eth_inv_returns, eth_inv_apy from investment_returns union all
  select 'Stablecoin Impact', fx_change, null from investment_returns
)

select
  i.label_tab, cs.eth_val, cs.apy
from items i
  left join cash_surplus cs on i.label = cs.label
order by i.fi_id
