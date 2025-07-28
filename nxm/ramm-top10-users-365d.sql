select
  case
    when starts_with(user, '0x') then concat(substring(user, 1, 6), '..', substring(user, length(user) - 3, 4))
    else user
  end as user,
  sum(amount) as amount,
  --min(block_date) as first_swap,
  --max(block_date) as last_swap,
  count(distinct tx_hash) as swap_count,
  avg(amount) as mean_amount,
  approx_percentile(amount, 0.5) as median_amount
from query_5547032 -- ramm swaps - base
where block_date >= current_date - interval '365' day
group by 1
order by 2 desc
limit 10
