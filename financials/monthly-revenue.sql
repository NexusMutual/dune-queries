with

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
    eth_inv_apy,
    eth_steth_apy,
    eth_reth_apy,
    eth_nxmty_apy,
    eth_aave_net_apy,
    eth_aweth_apy,
    eth_debt_usdc_apy,
    usd_inv_returns,
    usd_steth_return,
    usd_reth_return,
    usd_nxmty_return,
    usd_aave_net_return,
    usd_aweth_return,
    usd_debt_usdc_return,
    usd_inv_apy,
    usd_steth_apy,
    usd_reth_apy,
    usd_nxmty_apy,
    usd_aave_net_apy,
    usd_aweth_apy,
    usd_debt_usdc_apy,  
    eth_fx_change,
    usd_fx_change
  from query_4770697 -- investement returns
  where block_month >= date_add('month', -12, date_trunc('month', current_date))
    and block_month < date_trunc('month', current_date)
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
  where cover_month >= date_add('month', -12, date_trunc('month', current_date))
    and cover_month < date_trunc('month', current_date)
),

capital_movement as (
  select
    block_month,
    eth_eth_in,
    usd_eth_in,
    eth_eth_out,
    usd_eth_out
  from query_4841361 -- ramm volume
  where block_month >= date_add('month', -12, date_trunc('month', current_date))
    and block_month < date_trunc('month', current_date)
)

select
  ir.block_month,
  -- === ETH ===
  -- subtotals
  cs.eth_premium + cs.eth_member_fee + cs.eth_claim_paid + cs.eth_reimbursement as eth_cash_surplus,
  ir.eth_inv_returns + ir.eth_fx_change as eth_inv_returns_total,
  cm.eth_eth_in + cm.eth_eth_out as eth_capital_movement,
  cs.eth_premium + cs.eth_member_fee + cs.eth_claim_paid + cs.eth_reimbursement + ir.eth_inv_returns + ir.eth_fx_change + cm.eth_eth_in + cm.eth_eth_out as eth_cash_movement_total,
  -- cash surplus
  cs.eth_premium,
  cs.eth_member_fee,
  cs.eth_claim_paid + cs.eth_reimbursement as eth_claim_paid,
  -- inv returns
  ir.eth_inv_returns,
  ir.eth_steth_return,
  ir.eth_reth_return,
  ir.eth_nxmty_return,
  ir.eth_aave_net_return,
  ir.eth_aweth_return,
  ir.eth_debt_usdc_return,
  ir.eth_inv_apy,
  ir.eth_steth_apy,
  ir.eth_reth_apy,
  ir.eth_nxmty_apy,
  ir.eth_aave_net_apy,
  ir.eth_aweth_apy,
  ir.eth_debt_usdc_apy,
  ir.eth_fx_change,
  -- capital movement
  cm.eth_eth_in,
  cm.eth_eth_out,
  -- === USD ===
  -- subtotals
  cs.usd_premium + cs.usd_member_fee + cs.usd_claim_paid + cs.usd_reimbursement as usd_cash_surplus,
  ir.usd_inv_returns + ir.usd_fx_change as usd_inv_returns_total,
  cm.usd_eth_in + cm.usd_eth_out as usd_capital_movement,
  cs.usd_premium + cs.usd_member_fee + cs.usd_claim_paid + cs.usd_reimbursement + ir.usd_inv_returns + ir.usd_fx_change + cm.usd_eth_in + cm.usd_eth_out as usd_cash_movement_total,
  -- cash surplus
  cs.usd_premium,
  cs.usd_member_fee,
  cs.usd_claim_paid + cs.usd_reimbursement as usd_claim_paid,
  -- inv returns
  ir.usd_inv_returns,
  ir.usd_steth_return,
  ir.usd_reth_return,
  ir.usd_nxmty_return,
  ir.usd_aave_net_return,
  ir.usd_aweth_return,
  ir.usd_debt_usdc_return,
  ir.usd_inv_apy,
  ir.usd_steth_apy,
  ir.usd_reth_apy,
  ir.usd_nxmty_apy,
  ir.usd_aave_net_apy,
  ir.usd_aweth_apy,
  ir.usd_debt_usdc_apy,
  ir.usd_fx_change,
  -- capital movement
  cm.usd_eth_in,
  cm.usd_eth_out
from investment_returns ir
  left join cash_surplus cs on ir.block_month = cs.cover_month
  left join capital_movement cm on ir.block_month = cm.block_month
order by 1 desc
