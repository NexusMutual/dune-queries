select
  case
    when cast(d.seq_date as timestamp) = date_trunc('week', current_date) then 'current week'
    when cast(d.seq_date as timestamp) = date_add('week', -1, date_trunc('week', current_date)) then 'last week'
    else cast(d.seq_date as varchar)
  end as period_date
from (
    select sequence(
        date_add('week', -13, date_trunc('week', current_date)),
        date_trunc('week', current_date),
        interval '7' day
      ) as days
  ) as days_s
  cross join unnest(days) as d(seq_date)
order by cast(d.seq_date as timestamp) desc
