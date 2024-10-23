select
  case
    when cast(d.seq_date as timestamp) = date_trunc('quarter', current_date) then 'current quarter'
    when cast(d.seq_date as timestamp) = date_add('quarter', -1, date_trunc('quarter', current_date)) then 'last quarter'
    else cast(d.seq_date as varchar)
  end as period_date
from (
    select sequence(
        date_add('quarter', -10, date_trunc('quarter', current_date)),
        date_trunc('quarter', current_date),
        interval '3' month
      ) as days
  ) as days_s
  cross join unnest(days) as d(seq_date)
order by cast(d.seq_date as timestamp) desc
