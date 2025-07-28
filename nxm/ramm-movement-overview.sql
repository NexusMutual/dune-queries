with nxm_transfers as (
  select
    block_time,
    block_number,
    tx_from,
    tx_to,
    transfer_from,
    transfer_to,
    amount,
    evt_index,
    tx_hash
  from query_5531182 -- nxm transfers - base
  where tx_to = 'NM: RAMM'
)

select
  transfer_from as user,
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
from nxm_transfers
where transfer_to = 'origincity.eth' -- swaps below
group by 1
order by 2 desc
