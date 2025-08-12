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
    tu.address_label_formatted as address,
    sum(h.amount) as amount
  from query_5616437 h -- nxm combined history - base
    inner join tge_users tu on h.address = tu.address
  group by 1, 2
)

select
  block_date,
  address,
  amount
from nxm_combined_history
order by 1, 2
