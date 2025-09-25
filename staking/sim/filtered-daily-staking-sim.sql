with

params as (
  select
    cast({{ stake_amount }} as double) as stake_amount,
    cast({{ start_date }} as date) as start_date,
    cast({{ end_date }} as date) as end_date
),

adjusted as (
  select
    cast(s.date as date) as date,
    s.pool_id,
    s.pool_name,
    s.total_staked_nxm + case
      when s.date between p.start_date and p.end_date
      then p.stake_amount
      else 0
    end as total_staked_nxm_adj,
    s.reward_total,
    s.apy,
    s.apy_7d_ma,
    s.apy_30d_ma,
    s.apy_91d_ma
  from filtered_daily_staking s
    inner join params p on true
),

final as (
  select
    date,
    pool_id,
    pool_name,
    total_staked_nxm_adj as total_staked_nxm,
    reward_total,
    apy as baseline_apy,
    apy_7d_ma as baseline_apy_7d_ma,
    apy_30d_ma as baseline_apy_30d_ma,
    apy_91d_ma as baseline_apy_91d_ma,
    (reward_total / nullif(total_staked_nxm_adj, 0)) * 36500.0 as apy,
    avg((reward_total / nullif(total_staked_nxm_adj, 0)) * 36500.0) over (
      partition by pool_id
      order by date
      rows between 6 preceding and current row
    ) as apy_7d_ma,
    avg((reward_total / nullif(total_staked_nxm_adj, 0)) * 36500.0) over (
      partition by pool_id
      order by date
      rows between 29 preceding and current row
    ) as apy_30d_ma,
    avg((reward_total / nullif(total_staked_nxm_adj, 0)) * 36500.0) over (
      partition by pool_id
      order by date
      rows between 90 preceding and current row
    ) as apy_91d_ma
  from adjusted
)

select
  date,
  pool_id,
  pool_name,
  total_staked_nxm,
  reward_total,
  baseline_apy,
  baseline_apy_7d_ma,
  baseline_apy_30d_ma,
  baseline_apy_91d_ma,
  apy,
  apy_7d_ma,
  apy_30d_ma,
  apy_91d_ma
from final
order by 1, 2
