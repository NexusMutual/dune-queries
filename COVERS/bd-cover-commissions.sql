with

commissions as (
  select
    date_trunc('month', cover_start_date) as cover_month,
    commission_destination,
    sum(commission) as commission
  from query_3788370
  where commission > 0
  group by 1, 2
)

select
  cover_month,
  case
    when c.commission_destination = 0x586b9b2f8010b284a0197f392156f1a7eb5e86e9 then 'Community Fund'
    else coalesce(ens.name, cast(c.commission_destination as varchar))
  end as commission_destination,
  c.commission
from commissions c
  left join labels.ens on c.commission_destination = ens.address
order by 1, 2
