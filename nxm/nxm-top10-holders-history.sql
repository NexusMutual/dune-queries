with

top_holders as (
  select address, address_label
  from query_5530985 -- nxm holdings
  order by total_amount desc
  limit 10
),

nxm_combined_history as (
  select
    h.block_date,
    coalesce(th.address_label, cast(h.address as varchar)) as address,
    sum(h.amount) as amount
  from query_5616437 h -- nxm combined history - base
    inner join top_holders th on h.address = th.address
  group by 1, 2
)

select
  block_date,
  case
    when starts_with(address, '0x') then concat(substring(address, 1, 6), '..', substring(address, length(address) - 3, 4))
    else address
  end as address,
  amount
from nxm_combined_history
order by 1, 2
