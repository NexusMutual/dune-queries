with

params as (
  select cast(case '{{month}}'
      when 'current month' then cast(date_trunc('month', current_date) as varchar)
      when 'last month' then cast(date_add('month', -1, date_trunc('month', current_date)) as varchar)
      else '{{month}}'
    end as timestamp) as report_month
),

items as (
  select fi_id, label, label_tab
  from query_4832890 -- fin items
  where scope = 'rs'
),

investment_returns as (
  select
    block_month,
    eth_inv_returns,
    eth_steth_return,
    eth_reth_return,
    eth_nxmty_return,
    eth_aave_net_return,
    eth_aweth_return,
    eth_debt_usdc_return,
    usd_inv_returns,
    usd_steth_return,
    usd_reth_return,
    usd_nxmty_return,
    usd_aave_net_return,
    usd_aweth_return,
    usd_debt_usdc_return,
    eth_fx_change,
    usd_fx_change
  from query_4770697 -- investement returns
  where block_month in (select report_month from params)
),

cash_surplus as (
  select
    cover_month,
    eth_premium,
    usd_premium,
    eth_claim_paid,
    usd_claim_paid,
    eth_member_fee,
    usd_member_fee
  from query_4836553 -- cash surplus
  where cover_month in (select report_month from params)
),

capital_movement as (
  select
    block_month,
    eth_eth_in,
    usd_eth_in,
    eth_eth_out,
    usd_eth_out
  from query_4841361 -- ramm volume
  where block_month in (select report_month from params)
),

fin_combined as (
  select
    ir.eth_inv_returns,
    ir.eth_steth_return,
    ir.eth_reth_return,
    ir.eth_nxmty_return,
    ir.eth_aave_net_return,
    ir.eth_aweth_return,
    ir.eth_debt_usdc_return,
    ir.usd_inv_returns,
    ir.usd_steth_return,
    ir.usd_reth_return,
    ir.usd_nxmty_return,
    ir.usd_aave_net_return,
    ir.usd_aweth_return,
    ir.usd_debt_usdc_return,
    ir.eth_fx_change,
    ir.usd_fx_change,
    cs.eth_premium,
    cs.usd_premium,
    cs.eth_claim_paid,
    cs.usd_claim_paid,
    cs.eth_member_fee,
    cs.usd_member_fee,
    cm.eth_eth_in,
    cm.usd_eth_in,
    cm.eth_eth_out,
    cm.usd_eth_out
  from investment_returns ir
    cross join cash_surplus cs
    cross join capital_movement cm
)

select
  i.label_tab,
  case i.label
    -- revenue
    when 'Revenue Statement' then null
    when 'Cash Surplus' then eth_premium + eth_member_fee + eth_claim_paid
    when 'Premiums' then coalesce(nullif(eth_premium, 0), 1e-6)
    when 'Membership Fees' then coalesce(nullif(eth_member_fee, 0), 1e-6)
    when 'Claims - Reimbursements' then coalesce(nullif(eth_claim_paid, 0), 1e-6)
    when 'Investments Return Total' then eth_inv_returns + eth_fx_change
    when 'Total ETH Earned' then coalesce(nullif(eth_inv_returns, 0), 1e-6)
    when 'stETH Return' then coalesce(nullif(eth_steth_return, 0), 1e-6)
    when 'rETH Return' then coalesce(nullif(eth_reth_return, 0), 1e-6)
    when 'Enzyme Vault Return' then coalesce(nullif(eth_nxmty_return, 0), 1e-6)
    when 'Aave Net Return' then coalesce(nullif(eth_aave_net_return, 0), 1e-6)
    when 'aEthWETH Return' then coalesce(nullif(eth_aweth_return, 0), 1e-6)
    when 'debtUSDC Return' then coalesce(nullif(eth_debt_usdc_return, 0), 1e-6)
    when 'FX Impact' then coalesce(nullif(eth_fx_change, 0), 1e-6)
    when 'Capital Movement' then eth_eth_in + eth_eth_out
    when 'Contributions' then coalesce(nullif(eth_eth_in, 0), 1e-6)
    when 'Withdrawals' then coalesce(nullif(eth_eth_out, 0), 1e-6)
    when 'Total Cash Movement' then eth_premium + eth_member_fee + eth_claim_paid + eth_inv_returns + eth_fx_change + eth_eth_in + eth_eth_out
  end as eth_val,
  case i.label
    -- revenue
    when 'Revenue Statement' then null
    when 'Cash Surplus' then usd_premium + usd_member_fee + usd_claim_paid
    when 'Premiums' then coalesce(nullif(usd_premium, 0), 1e-6)
    when 'Membership Fees' then coalesce(nullif(usd_member_fee, 0), 1e-6)
    when 'Claims - Reimbursements' then coalesce(nullif(usd_claim_paid, 0), 1e-6)
    when 'Investments Return Total' then usd_inv_returns + usd_fx_change
    when 'Total ETH Earned' then coalesce(nullif(usd_inv_returns, 0), 1e-6)
    when 'stETH Return' then coalesce(nullif(usd_steth_return, 0), 1e-6)
    when 'rETH Return' then coalesce(nullif(usd_reth_return, 0), 1e-6)
    when 'Enzyme Vault Return' then coalesce(nullif(usd_nxmty_return, 0), 1e-6)
    when 'Aave Net Return' then coalesce(nullif(usd_aave_net_return, 0), 1e-6)
    when 'aEthWETH Return' then coalesce(nullif(usd_aweth_return, 0), 1e-6)
    when 'debtUSDC Return' then coalesce(nullif(usd_debt_usdc_return, 0), 1e-6)
    when 'FX Impact' then coalesce(nullif(usd_fx_change, 0), 1e-6)
    when 'Capital Movement' then usd_eth_in + usd_eth_out
    when 'Contributions' then coalesce(nullif(usd_eth_in, 0), 1e-6)
    when 'Withdrawals' then coalesce(nullif(usd_eth_out, 0), 1e-6)
    when 'Total Cash Movement' then usd_premium + usd_member_fee + usd_claim_paid + usd_inv_returns + usd_fx_change + usd_eth_in + usd_eth_out
  end as usd_val
from fin_combined fc
  cross join items i
order by i.fi_id
