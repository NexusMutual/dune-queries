select
  case
    when total_amount > 25000 then
      case
        when address_label is null
        then concat(substring(cast(address as varchar), 1, 6), '..', substring(cast(address as varchar), length(cast(address as varchar)) - 3, 4))
        else address_label
      end
    else 'Others'
  end as address,
  sum(total_amount) as amount
from query_5530985 -- nxm holdings
group by 1
order by 2 desc
