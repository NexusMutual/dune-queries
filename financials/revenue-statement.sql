with

params as (
  select cast(case '{{month}}'
      when 'current MTD ⏳' then cast(date_trunc('month', current_date) as varchar)
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
    eth_capital_pool_start,
    eth_capital_pool_end,
    eth_inv_returns,
    eth_steth_return,
    eth_reth_return,
    eth_nxmty_return,
    eth_aave_net_return,
    eth_aweth_return,
    eth_debt_usdc_return,
    usd_capital_pool_start,
    usd_capital_pool_end,
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
    eth_member_fee,
    usd_member_fee,
    eth_claim_paid,
    usd_claim_paid,
    eth_reimbursement,
    usd_reimbursement
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
    ir.eth_capital_pool_start,
    ir.eth_capital_pool_end,
    ir.eth_inv_returns,
    ir.eth_steth_return,
    ir.eth_reth_return,
    ir.eth_nxmty_return,
    ir.eth_aave_net_return,
    ir.eth_aweth_return,
    ir.eth_debt_usdc_return,
    ir.usd_capital_pool_start,
    ir.usd_capital_pool_end,
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
    cs.eth_member_fee,
    cs.usd_member_fee,
    cs.eth_claim_paid,
    cs.usd_claim_paid,
    cs.eth_reimbursement,
    cs.usd_reimbursement,
    cm.eth_eth_in,
    cm.usd_eth_in,
    cm.eth_eth_out,
    cm.usd_eth_out,
    -- ETH totals
    cs.eth_premium + cs.eth_member_fee + cs.eth_claim_paid + cs.eth_reimbursement as eth_cash_surplus,
    ir.eth_inv_returns + ir.eth_fx_change as eth_inv_returns_total,
    cm.eth_eth_in + cm.eth_eth_out as eth_capital_movement,
    cs.eth_premium + cs.eth_member_fee + cs.eth_claim_paid + cs.eth_reimbursement + ir.eth_inv_returns + ir.eth_fx_change + cm.eth_eth_in + cm.eth_eth_out as eth_cash_movement,
    -- USD totals
    cs.usd_premium + cs.usd_member_fee + cs.usd_claim_paid + cs.usd_reimbursement as usd_cash_surplus,
    cm.usd_eth_in + cm.usd_eth_out as usd_capital_movement,
    cs.usd_premium + cs.usd_member_fee + cs.usd_claim_paid + cs.usd_reimbursement + ir.usd_inv_returns + cm.usd_eth_in + cm.usd_eth_out as usd_cash_movement -- exclude fx impact here (use adjusted calc below)
  from investment_returns ir
    cross join cash_surplus cs
    cross join capital_movement cm
),

fin_combined_ext as (
  select
    *,
    -- FX Impact = Closing Balance - Capital Movement - Total ETH Earned - Cash Surplus - Opening Balance
    usd_capital_pool_end - usd_capital_movement - usd_inv_returns - usd_cash_surplus - usd_capital_pool_start as usd_fx_change_adjusted
  from fin_combined
)

select
  i.label_tab,
  case i.label
    -- revenue
    when 'Revenue Statement' then null
    when 'Cash Surplus' then coalesce(nullif(eth_cash_surplus, 0), 1e-6)
    when 'Premiums' then coalesce(nullif(eth_premium, 0), 1e-6)
    when 'Membership Fees' then coalesce(nullif(eth_member_fee, 0), 1e-6)
    when 'Claims - Reimbursements' then coalesce(nullif(eth_claim_paid + eth_reimbursement, 0), 1e-6)
    when 'Investments Return Total' then coalesce(nullif(eth_inv_returns_total, 0), 1e-6)
    when 'Total ETH Earned' then coalesce(nullif(eth_inv_returns, 0), 1e-6)
    when 'stETH Return' then coalesce(nullif(eth_steth_return, 0), 1e-6)
    when 'rETH Return' then coalesce(nullif(eth_reth_return, 0), 1e-6)
    when 'Enzyme Vault Return' then coalesce(nullif(eth_nxmty_return, 0), 1e-6)
    when 'Aave Net Return' then coalesce(nullif(eth_aave_net_return, 0), 1e-6)
    when 'aEthWETH Return' then coalesce(nullif(eth_aweth_return, 0), 1e-6)
    when 'debtUSDC Return' then coalesce(nullif(eth_debt_usdc_return, 0), 1e-6)
    when 'FX Impact' then coalesce(nullif(eth_fx_change, 0), 1e-6)
    when 'Capital Movement' then coalesce(nullif(eth_capital_movement, 0), 1e-6)
    when 'Contributions' then coalesce(nullif(eth_eth_in, 0), 1e-6)
    when 'Withdrawals' then coalesce(nullif(eth_eth_out, 0), 1e-6)
    when 'Total Cash Movement' then coalesce(nullif(eth_cash_movement, 0), 1e-6)
    when 'Reconcilation Difference' then eth_capital_pool_end - eth_capital_pool_start - eth_cash_movement
    when 'Total Cash Movement After Rec Diff' then eth_capital_pool_end - eth_capital_pool_start
  end as eth_val,
  case i.label
    -- revenue
    when 'Revenue Statement' then null
    when 'Cash Surplus' then coalesce(nullif(usd_cash_surplus, 0), 1e-6)
    when 'Premiums' then coalesce(nullif(usd_premium, 0), 1e-6)
    when 'Membership Fees' then coalesce(nullif(usd_member_fee, 0), 1e-6)
    when 'Claims - Reimbursements' then coalesce(nullif(usd_claim_paid + usd_reimbursement, 0), 1e-6)
    when 'Investments Return Total' then usd_inv_returns + usd_fx_change_adjusted
    when 'Total ETH Earned' then coalesce(nullif(usd_inv_returns, 0), 1e-6)
    when 'stETH Return' then coalesce(nullif(usd_steth_return, 0), 1e-6)
    when 'rETH Return' then coalesce(nullif(usd_reth_return, 0), 1e-6)
    when 'Enzyme Vault Return' then coalesce(nullif(usd_nxmty_return, 0), 1e-6)
    when 'Aave Net Return' then coalesce(nullif(usd_aave_net_return, 0), 1e-6)
    when 'aEthWETH Return' then coalesce(nullif(usd_aweth_return, 0), 1e-6)
    when 'debtUSDC Return' then coalesce(nullif(usd_debt_usdc_return, 0), 1e-6)
    when 'FX Impact' then coalesce(nullif(usd_fx_change_adjusted, 0), 1e-6)
    when 'Capital Movement' then coalesce(nullif(usd_capital_movement, 0), 1e-6)
    when 'Contributions' then coalesce(nullif(usd_eth_in, 0), 1e-6)
    when 'Withdrawals' then coalesce(nullif(usd_eth_out, 0), 1e-6)
    when 'Total Cash Movement' then usd_cash_movement + usd_fx_change_adjusted
    when 'Reconcilation Difference'
      then if(
        abs(usd_capital_pool_end - usd_capital_pool_start - (usd_cash_movement + usd_fx_change_adjusted)) < 1e-6,
        1e-6,
        usd_capital_pool_end - usd_capital_pool_start - (usd_cash_movement + usd_fx_change_adjusted)
      )
    when 'Total Cash Movement After Rec Diff' then usd_capital_pool_end - usd_capital_pool_start
  end as usd_val
from fin_combined_ext fc
  cross join items i
order by i.fi_id
