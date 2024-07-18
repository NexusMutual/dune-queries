with

commissions as (
  select
    commission_destination,
    sum(commission) as commission
  from query_3788370
  group by 1
)

select
  coalesce(ens.name, cast(c.commission_destination as varchar)) as commission_destination,
  commission
from commissions c
  left join labels.ens on c.commission_destination = ens.address
