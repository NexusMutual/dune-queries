with

items as (
  select fi_id, scope, label, label_tab
  from query_4832890 -- fin items
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
  where block_month = timestamp '2025-02-01'
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
  where cover_month = timestamp '2025-02-01'
),

capital_movement as (
  select
    block_month,
    eth_eth_in,
    usd_eth_in,
    eth_eth_out,
    usd_eth_out
  from query_4841361 -- ramm volume
  where block_month = timestamp '2025-02-01'
),

balance_sheet as (
  select
    block_month,
    eth_capital_pool,
    eth_eth,
    eth_steth,
    eth_reth,
    eth_nxmty,
    eth_aweth,
    eth_debt_usdc,
    eth_dai,
    eth_usdc,
    eth_cbbtc,
    eth_cover_re,
    usd_capital_pool,
    usd_eth,
    usd_steth,
    usd_reth,
    usd_nxmty,
    usd_aweth,
    usd_debt_usdc,
    usd_dai,
    usd_usdc,
    usd_cbbtc,
    usd_cover_re
  from query_4841979 -- balance sheet
  where block_month = timestamp '2025-02-01'
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
    cm.usd_eth_out,
    bs.eth_capital_pool,
    bs.eth_eth,
    bs.eth_steth,
    bs.eth_reth,
    bs.eth_nxmty,
    bs.eth_aweth,
    bs.eth_debt_usdc,
    bs.eth_dai,
    bs.eth_usdc,
    bs.eth_cbbtc,
    bs.eth_cover_re,
    bs.usd_capital_pool,
    bs.usd_eth,
    bs.usd_steth,
    bs.usd_reth,
    bs.usd_nxmty,
    bs.usd_aweth,
    bs.usd_debt_usdc,
    bs.usd_dai,
    bs.usd_usdc,
    bs.usd_cbbtc,
    bs.usd_cover_re
  from investment_returns ir
    cross join cash_surplus cs
    cross join capital_movement cm
    cross join balance_sheet bs
)

select
  i.label_tab,
  case i.label
    -- revenue
    when 'Revenue Statement' then null
    when 'Cash Surplus' then eth_premium + eth_member_fee + eth_claim_paid
    when 'Premiums' then eth_premium
    when 'Membership Fees' then eth_member_fee
    when 'Claims - Reimbursements' then eth_claim_paid
    when 'Investments Total' then eth_inv_returns + eth_fx_change
    when 'Total ETH Earned' then eth_inv_returns
    when 'stETH Return' then eth_steth_return
    when 'rETH Return' then eth_reth_return
    when 'Enzyme Vault Return' then eth_nxmty_return
    when 'Aave Net Return' then eth_aave_net_return
    when 'aEthWETH Return' then eth_aweth_return
    when 'debtUSDC Return' then eth_debt_usdc_return
    when 'FX Impact' then eth_fx_change
    when 'Capital Movement' then eth_eth_in + eth_eth_out
    when 'Contributions' then eth_eth_in
    when 'Withdrawals' then eth_eth_out
    when 'Total Cash Movement' then eth_premium + eth_member_fee + eth_claim_paid + eth_inv_returns + eth_fx_change + eth_eth_in + eth_eth_out
    -- balance sheet
    when 'Balance Sheet' then eth_capital_pool
    when 'Crypto Denominated Assets' then eth_eth + eth_steth + eth_reth + eth_cbbtc + eth_nxmty + eth_aweth
    when 'ETH' then eth_eth
    when 'stETH' then eth_steth
    when 'rETH' then eth_reth
    when 'cbBTC' then eth_cbbtc
    when 'Enzyme Vault' then eth_nxmty
    when 'Aave aEthWETH' then eth_aweth
    when 'Stablecoin Denominated Assets' then eth_dai + eth_usdc + eth_cover_re + eth_debt_usdc
    when 'DAI' then eth_dai
    when 'USDC' then eth_usdc
    when 'Cover Re' then eth_cover_re
    when 'Aave debtUSDC' then eth_debt_usdc
  end as eth_val,
  case i.label
    -- revenue
    when 'Revenue Statement' then null
    when 'Cash Surplus' then usd_premium + usd_member_fee + usd_claim_paid
    when 'Premiums' then usd_premium
    when 'Membership Fees' then usd_member_fee
    when 'Claims - Reimbursements' then usd_claim_paid
    when 'Investments Total' then usd_inv_returns + usd_fx_change
    when 'Total ETH Earned' then usd_inv_returns
    when 'stETH Return' then usd_steth_return
    when 'rETH Return' then usd_reth_return
    when 'Enzyme Vault Return' then usd_nxmty_return
    when 'Aave Net Return' then usd_aave_net_return
    when 'aEthWETH Return' then usd_aweth_return
    when 'debtUSDC Return' then usd_debt_usdc_return
    when 'FX Impact' then usd_fx_change
    when 'Capital Movement' then usd_eth_in + usd_eth_out
    when 'Contributions' then usd_eth_in
    when 'Withdrawals' then usd_eth_out
    when 'Total Cash Movement' then usd_premium + usd_member_fee + usd_claim_paid + usd_inv_returns + usd_fx_change + usd_eth_in + usd_eth_out
    -- balance sheet
    when 'Balance Sheet' then usd_capital_pool
    when 'Crypto Denominated Assets' then usd_eth + usd_steth + usd_reth + usd_cbbtc + usd_nxmty + usd_aweth
    when 'ETH' then usd_eth
    when 'stETH' then usd_steth
    when 'rETH' then usd_reth
    when 'cbBTC' then usd_cbbtc
    when 'Enzyme Vault' then usd_nxmty
    when 'Aave aEthWETH' then usd_aweth
    when 'Stablecoin Denominated Assets' then usd_dai + usd_usdc + usd_cover_re + usd_debt_usdc
    when 'DAI' then usd_dai
    when 'USDC' then usd_usdc
    when 'Cover Re' then usd_cover_re
    when 'Aave debtUSDC' then usd_debt_usdc
  end as usd_val
from fin_combined fc
  cross join items i
order by i.fi_id
