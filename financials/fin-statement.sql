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

fin_combined as (
  select
    ir.eth_inv_returns,
    ir.fx_change,
    pc.eth_premiums_minus_claims,
    pc.usd_premiums_minus_claims,
    mf.eth_member_fee,
    mf.usd_member_fee
  from investment_returns ir, premiums_claims pc, member_fees mf
)

select
  i.label_tab,
  case i.label
    when 'Cash Surplus' then eth_inv_returns + fx_change + eth_premiums_minus_claims + eth_member_fee
    when 'Investments' then eth_inv_returns + fx_change
    when 'Investment Returns' then eth_inv_returns
    when 'Stablecoin Impact' then fx_change
    when 'Premiums - Claims' then eth_premiums_minus_claims
    when 'Membership Fees' then eth_member_fee
  end as eth_val
from fin_combined fc
  cross join items i
order by i.fi_id
