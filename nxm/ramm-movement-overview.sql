select
  user,
  sum(amount) as amount,
  min(block_time) as first_swap,
  max(block_time) as last_swap,
  count(distinct tx_hash) as swap_count,
  avg(amount) as mean_amount,
  approx_percentile(amount, 0.5) as median_amount,
  sum(amount) filter (where block_time >= current_date - interval '365' day) as amount_365d,
  sum(amount) filter (where block_time >= current_date - interval '180' day) as amount_180d,
  sum(amount) filter (where block_time >= current_date - interval '90' day) as amount_90d,
  sum(amount) filter (where block_time >= current_date - interval '30' day) as amount_30d,
  sum(amount) filter (where block_time >= current_date - interval '7' day) as amount_7d
from query_5547032 -- ramm swaps - base
group by 1
order by 2 desc
