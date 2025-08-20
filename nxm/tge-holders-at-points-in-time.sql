with

tge_users as (
  select
    address,
    address_label,
    address_label_formatted,
    initial_distribution
  from query_5620680 -- tge users
),

nxm_combined_history as (
  select
    h.block_date,
    h.address,
    sum(h.amount) as amount
  from query_5616437 h -- nxm combined history - base
  where h.address in (select address from tge_users)
  group by 1, 2
)

select
  concat(
    '<a href="https://etherscan.io/address/', cast(tu.address as varchar), '" target="_blank">👉 ',
    tu.address_label_formatted,
    ' 🔗</a>'
  ) as address,
  tu.initial_distribution,
  if(h_5y_ago.amount > 1e-6, h_5y_ago.amount, 0) as amount_5y_ago,
  if(h_4y_ago.amount > 1e-6, h_4y_ago.amount, 0) as amount_4y_ago,
  if(h_3y_ago.amount > 1e-6, h_3y_ago.amount, 0) as amount_3y_ago,
  if(h_2y_ago.amount > 1e-6, h_2y_ago.amount, 0) as amount_2y_ago,
  if(h_1y_ago.amount > 1e-6, h_1y_ago.amount, 0) as amount_1y_ago,
  if(h_now.amount > 1e-6, h_now.amount, 0) as amount_now
from tge_users tu
  inner join nxm_combined_history h_now on tu.address = h_now.address and h_now.block_date = current_date
  inner join nxm_combined_history h_1y_ago on tu.address = h_1y_ago.address and h_1y_ago.block_date = current_date - interval '1' year
  inner join nxm_combined_history h_2y_ago on tu.address = h_2y_ago.address and h_2y_ago.block_date = current_date - interval '2' year
  inner join nxm_combined_history h_3y_ago on tu.address = h_3y_ago.address and h_3y_ago.block_date = current_date - interval '3' year
  inner join nxm_combined_history h_4y_ago on tu.address = h_4y_ago.address and h_4y_ago.block_date = current_date - interval '4' year
  inner join nxm_combined_history h_5y_ago on tu.address = h_5y_ago.address and h_5y_ago.block_date = current_date - interval '5' year
order by tu.initial_distribution desc
