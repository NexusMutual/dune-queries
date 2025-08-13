with

params as (
  select
    cast(100 as double) as holder_threshold,
    cast(0.5 as double) as retention_1y_cutoff,
    cast(0.6 as double) as retention_2y_cutoff,
    cast(0.7 as double) as retention_3y_cutoff
),

nxm_combined_history as (
  select
    h.block_date,
    h.address,
    sum(h.amount) as amount
  from query_5616437 h -- nxm combined history - base
  group by 1, 2
),

base as (
  select
    h_now.address,
    if(h_5y_ago.amount > 1e-6, h_5y_ago.amount, 0) as amount_5y_ago,
    if(h_4y_ago.amount > 1e-6, h_4y_ago.amount, 0) as amount_4y_ago,
    if(h_3y_ago.amount > 1e-6, h_3y_ago.amount, 0) as amount_3y_ago,
    if(h_2y_ago.amount > 1e-6, h_2y_ago.amount, 0) as amount_2y_ago,
    if(h_1y_ago.amount > 1e-6, h_1y_ago.amount, 0) as amount_1y_ago,
    if(h_now.amount > 1e-6, h_now.amount, 0) as amount_now
  from nxm_combined_history h_now
    inner join nxm_combined_history h_1y_ago on h_now.address = h_1y_ago.address and h_1y_ago.block_date = current_date - interval '1' year
    inner join nxm_combined_history h_2y_ago on h_now.address = h_2y_ago.address and h_2y_ago.block_date = current_date - interval '2' year
    left join nxm_combined_history h_3y_ago on h_now.address = h_3y_ago.address and h_3y_ago.block_date = current_date - interval '3' year
    left join nxm_combined_history h_4y_ago on h_now.address = h_4y_ago.address and h_4y_ago.block_date = current_date - interval '4' year
    left join nxm_combined_history h_5y_ago on h_now.address = h_5y_ago.address and h_5y_ago.block_date = current_date - interval '5' year
  where h_now.block_date = current_date
)

select
  b.address,
  b.amount_now,
  b.amount_1y_ago,
  b.amount_2y_ago,
  coalesce(b.amount_3y_ago, 0) as amount_3y_ago,
  coalesce(b.amount_4y_ago, 0) as amount_4y_ago,
  coalesce(b.amount_5y_ago, 0) as amount_5y_ago,

  -- retention stats
  b.amount_now / nullif(b.amount_1y_ago, 0) as retention_ratio_1y,
  b.amount_now / nullif(b.amount_2y_ago, 0) as retention_ratio_2y,
  b.amount_now / nullif(b.amount_3y_ago, 0) as retention_ratio_3y,

  -- base lth: >=100 now & 1y ago, and kept >=50% since last year
  case
    when b.amount_now >= p.holder_threshold
     and b.amount_1y_ago >= p.holder_threshold
     and (b.amount_now / nullif(b.amount_1y_ago, 0)) >= p.retention_1y_cutoff
    then true
    else false
  end as is_long_term_holder,

  -- upgraded tiers: 2y >=60%, 3y >=70% (still meeting base lth)
  case
    when b.amount_now >= p.holder_threshold
     and b.amount_1y_ago >= p.holder_threshold
     and (b.amount_now / nullif(b.amount_1y_ago, 0)) >= p.retention_1y_cutoff
     and b.amount_3y_ago >= p.holder_threshold
     and (b.amount_now / nullif(b.amount_3y_ago, 0)) >= p.retention_3y_cutoff
    then 'strong_3y_70'
    when b.amount_now >= p.holder_threshold
     and b.amount_1y_ago >= p.holder_threshold
     and (b.amount_now / nullif(b.amount_1y_ago, 0)) >= p.retention_1y_cutoff
     and b.amount_2y_ago >= p.holder_threshold
     and (b.amount_now / nullif(b.amount_2y_ago, 0)) >= p.retention_2y_cutoff
    then 'strong_2y_60'
    when b.amount_now >= p.holder_threshold
     and b.amount_1y_ago >= p.holder_threshold
     and (b.amount_now / nullif(b.amount_1y_ago, 0)) >= p.retention_1y_cutoff
    then 'base_1y_50'
    else 'no'
  end as lth_tier,

  -- extras
  b.amount_now - b.amount_1y_ago as net_change_1y,
  b.amount_now - b.amount_2y_ago as net_change_2y,
  b.amount_now - coalesce(b.amount_3y_ago, 0) as net_change_3y,

  (case when b.amount_now >= p.holder_threshold then 1 else 0 end
   + case when b.amount_1y_ago >= p.holder_threshold then 1 else 0 end
   + case when b.amount_2y_ago >= p.holder_threshold then 1 else 0 end
   + case when coalesce(b.amount_3y_ago, 0) >= p.holder_threshold then 1 else 0 end
   + case when coalesce(b.amount_4y_ago, 0) >= p.holder_threshold then 1 else 0 end
   + case when coalesce(b.amount_5y_ago, 0) >= p.holder_threshold then 1 else 0 end) as holding_years_count,

  dense_rank() over (
    order by
      case
        when b.amount_now >= p.holder_threshold
         and b.amount_1y_ago >= p.holder_threshold
         and (b.amount_now / nullif(b.amount_1y_ago, 0)) >= p.retention_1y_cutoff
        then 1 else 0 end desc,
      b.amount_now / nullif(b.amount_3y_ago, 0) desc,
      b.amount_now / nullif(b.amount_2y_ago, 0) desc,
      b.amount_now / nullif(b.amount_1y_ago, 0) desc,
      b.amount_now desc
  ) as lth_rank

from base b
  cross join params p
where b.amount_now > 0
  or b.amount_1y_ago > 0
  or b.amount_2y_ago > 0
  or coalesce(b.amount_3y_ago, 0) > 0
  or coalesce(b.amount_4y_ago, 0) > 0
  or coalesce(b.amount_5y_ago, 0) > 0;
