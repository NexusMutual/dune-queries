select
  block_month,
  eth_kiln_rewards_total,
  eth_kiln_rewards_total_prev,
  eth_kiln_rewards_total - eth_kiln_rewards_total_prev as eth_kiln_rewards,
  case
    when block_month = timestamp '2024-05-01' then 2.2632 -- Apr final total post 1st round withdrawal
    when block_month = timestamp '2024-06-01' then 17.5398 -- May final total post 1st round withdrawal
    else eth_kiln_rewards_total_prev - lag(eth_kiln_rewards_total, 2, 0) over (order by block_month)
  end as eth_kiln_rewards_prev
from (
  select
    date_trunc('month', t.seq_date) as block_month,
    case
      when t.seq_date = timestamp '2024-04-30' then 181.45 -- 1st round withdrawal
      else 0
    end + t.kiln_rewards as eth_kiln_rewards_total,
    lag(t.kiln_rewards, 1, 0) over (order by t.seq_date) as eth_kiln_rewards_total_prev
  from (
      select
        seq_date,
        kiln_rewards,
        row_number() over (partition by date_trunc('month', seq_date) order by seq_date desc) as rn
      from query_4830965 -- kiln rewards
    ) t
  where t.rn = 1
) k
order by 1 desc
