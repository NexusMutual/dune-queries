with top10_users as (
  select
    user,
    sum(amount) as amount
  from query_5547032 -- ramm swaps - base
  where block_date >= current_date - interval '90' day
  group by 1
  order by 2 desc
  limit 10
)

select
  block_date,
  flow_type,
  case
    when starts_with(user, '0x') then concat(substring(user, 1, 6), '..', substring(user, length(user) - 3, 4))
    else user
  end as user,
  sum(amount) as amount
from query_5547032 -- ramm swaps - base
where user in (select user from top10_users)
  and block_date >= current_date - interval '90' day
group by 1, 2, 3
order by 1, 2, 3
