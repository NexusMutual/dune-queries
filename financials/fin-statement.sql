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

ramm_volume as (
  select
    block_month,
    eth_nxm_volume,
    usd_nxm_volume,
    eth_eth_volume,
    usd_eth_volume
  from query_4841361 -- ramm volume
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
    rv.eth_nxm_volume,
    rv.usd_nxm_volume,
    rv.eth_eth_volume,
    rv.usd_eth_volume
  from investment_returns ir
    cross join premiums_claims pc
    cross join member_fees mf
    cross join ramm_volume rv
)

select
  i.label_tab,
  case i.label
    when 'Revenue Statement' then null
    when 'Cash Surplus' then eth_inv_returns + fx_change + eth_premiums_minus_claims + eth_member_fee
    when 'Investments' then eth_inv_returns + fx_change
    when 'Investment Returns' then eth_inv_returns
    when 'Stablecoin Impact' then fx_change
    when 'Premiums - Claims' then eth_premiums_minus_claims
    when 'Membership Fees' then eth_member_fee
    when 'Reserve Movement' then null
    when 'Capital Movement' then eth_nxm_volume - eth_eth_volume
    when 'Contributions' then eth_nxm_volume
    when 'Withdrawals' then eth_eth_volume
    when 'Balance Sheet' then null
    when 'ETH Denominated Assets' then null
    when 'ETH' then null
    when 'stETH' then null
    when 'rETH' then null
    when 'cbBTC' then null
    when 'Enzyme Vault' then null
    when 'Aave aEthWETH' then null
    when 'Stablecoin Denominated Assets' then null
    when 'DAI' then null
    when 'USDC' then null
    when 'Cover Re' then null
    when 'Aave debtUSDC' then null
  end as eth_val,
  null as usd_val
from fin_combined fc
  cross join items i
order by i.fi_id
