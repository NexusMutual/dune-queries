select
  case
    when cast(d.seq_date as timestamp) = date_trunc('month', current_date) then 'current month'
    when cast(d.seq_date as timestamp) = date_add('month', -1, date_trunc('month', current_date)) then 'last month'
    else cast(d.seq_date as varchar)
  end as period_date
from (
    select sequence(
        date_add('month', -12, date_trunc('month', current_date)),
        date_trunc('month', current_date),
        interval '1' month
      ) as days
  ) as days_s
  cross join unnest(days) as d(seq_date)
order by cast(d.seq_date as timestamp) desc
