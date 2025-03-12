with

items as (
  select fi_id, scope, label, label_tab
  from query_4832890 -- fin items
),

investment_returns as (
  select
    block_month,
    eth_inv_returns,
    fx_change
  from query_4770697 -- investement returns
  where block_month = timestamp '2025-02-01'
),

premiums_claims as (
  select
    cover_month,
    eth_premiums_minus_claims,
    usd_premiums_minus_claims
  from query_4836553 -- premiums - claims
  where cover_month = timestamp '2025-02-01'
),

member_fees as (
  select
    block_month,
    eth_member_fee,
    usd_member_fee
  from query_4836646 -- member fees
  where block_month = timestamp '2025-02-01'
),

capital_movement as (
  select
    block_month,
    eth_eth_in,
    eth_usd_in,
    eth_eth_out,
    eth_usd_out
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
    ir.fx_change,
    pc.eth_premiums_minus_claims,
    pc.usd_premiums_minus_claims,
    mf.eth_member_fee,
    mf.usd_member_fee,
    cm.eth_eth_in,
    cm.eth_usd_in,
    cm.eth_eth_out,
    cm.eth_usd_out,
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
    cross join premiums_claims pc
    cross join member_fees mf
    cross join capital_movement cm
    cross join balance_sheet bs
)

select
  i.label_tab,
  case i.label
    -- revenue
    when 'Revenue Statement' then eth_inv_returns + fx_change + eth_premiums_minus_claims + eth_member_fee + eth_eth_in + eth_eth_out
    when 'Cash Surplus' then eth_inv_returns + fx_change + eth_premiums_minus_claims + eth_member_fee
    when 'Investments' then eth_inv_returns + fx_change
    when 'Investment Returns' then eth_inv_returns
    when 'Stablecoin Impact' then fx_change
    when 'Premiums - Claims' then eth_premiums_minus_claims
    when 'Membership Fees' then eth_member_fee
    when 'Capital Movement' then eth_eth_in + eth_eth_out
    when 'Contributions' then eth_eth_in
    when 'Withdrawals' then eth_eth_out
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
  null as usd_val
from fin_combined fc
  cross join items i
order by i.fi_id
