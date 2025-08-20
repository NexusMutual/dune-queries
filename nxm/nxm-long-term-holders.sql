with

params as (
  select
    cast(100 as double) as holder_threshold,
    cast(0.7 as double) as retention_1y_cutoff,
    cast(0.6 as double) as retention_2y_cutoff,
    cast(0.5 as double) as retention_3y_cutoff,
    cast(0.4 as double) as retention_4y_cutoff,
    cast(0.3 as double) as retention_5y_cutoff,
    cast(0.6 as double) as weighted_cutoff,
    cast(0.6 as double) as balance_weight,
    cast(0.4 as double) as retention_weight,
    cast(100000 as double) as whale_threshold
),

nxm_combined_history as (
  select
    h.block_date,
    h.address,
    sum(h.amount) as amount
  from query_5616437 h -- nxm combined history - base
  group by 1, 2
),

address_labels as (
  select address, address_label from query_5534312
),

labels_contracts as (
  select
    address,
    case
      when lower(namespace) = 'wnxm' then name
      when lower(namespace) in ('gnosis_safe', 'gnosissafe', 'gnosis_multisig') then null
      else concat(namespace, ': ', name)
    end as contract_name
  from (
    select
      address, namespace, name,
      row_number() over (partition by address order by created_at desc) as rn
    from ethereum.contracts
    where namespace <> 'safe_test'
  ) t
  where rn = 1
),

base as (
  select
    h_now.address,
    coalesce(al.address_label, ens.name) as address_label,
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
    left join labels_contracts lc on h_now.address = lc.address
    left join address_labels al on h_now.address = al.address
    left join labels.ens on h_now.address = ens.address
  where h_now.block_date = current_date
    and lc.contract_name is null -- allow gnosis safe
    and coalesce(al.address_label, '') not like 'NM:%' -- exclude NM contracts/addresses
),

calc as (
  select
    concat(
      '<a href="https://etherscan.io/address/', cast(b.address as varchar), '" target="_blank">👉 ',
      case
        when b.address_label is null
        then concat(substring(cast(b.address as varchar), 1, 6), '..', substring(cast(b.address as varchar), length(cast(b.address as varchar)) - 5, 6))
        else b.address_label
      end,
      ' 🔗</a>'
    ) as address,
    b.amount_now,
    b.amount_1y_ago,
    b.amount_2y_ago,
    b.amount_3y_ago,
    b.amount_4y_ago,
    b.amount_5y_ago,
    -- capped retentions
    least(b.amount_now / nullif(b.amount_1y_ago, 0), 1) as r1_cap,
    least(b.amount_now / nullif(b.amount_2y_ago, 0), 1) as r2_cap,
    least(b.amount_now / nullif(b.amount_3y_ago, 0), 1) as r3_cap,
    least(b.amount_now / nullif(b.amount_4y_ago, 0), 1) as r4_cap,
    least(b.amount_now / nullif(b.amount_5y_ago, 0), 1) as r5_cap,
    -- retention score (still the gate)
    0.6 * least(b.amount_now / nullif(b.amount_1y_ago, 0), 1)
    + 0.3 * least(b.amount_now / nullif(b.amount_2y_ago, 0), 1)
    + 0.1 * least(b.amount_now / nullif(b.amount_3y_ago, 0), 1) as weighted_score,
    -- normalized balance (0..1) using max-now across cohort
    least(b.amount_now / nullif(max(b.amount_now) over (), 0), 1) as balance_pct,
    p.weighted_cutoff,
    p.balance_weight,
    p.retention_weight,
    -- final score for ranking: whales boosted
    p.balance_weight * least(b.amount_now / nullif(max(b.amount_now) over (), 0), 1)
    + p.retention_weight * (
        0.6 * least(b.amount_now / nullif(b.amount_1y_ago, 0), 1)
      + 0.3 * least(b.amount_now / nullif(b.amount_2y_ago, 0), 1)
      + 0.1 * least(b.amount_now / nullif(b.amount_3y_ago, 0), 1)
    ) as final_score,
    -- 70/60/50 gate badge
    case
      when b.amount_now >= p.holder_threshold
       and b.amount_1y_ago >= p.holder_threshold
       and b.amount_2y_ago >= p.holder_threshold
       and b.amount_3y_ago >= p.holder_threshold
       and least(b.amount_now / nullif(b.amount_1y_ago, 0), 1) >= p.retention_1y_cutoff
       and least(b.amount_now / nullif(b.amount_2y_ago, 0), 1) >= p.retention_2y_cutoff
       and least(b.amount_now / nullif(b.amount_3y_ago, 0), 1) >= p.retention_3y_cutoff
      then true else false
    end as badge_gate_70_60_50,
    -- optional legacy badges
    case
      when b.amount_4y_ago >= p.holder_threshold
       and least(b.amount_now / nullif(b.amount_4y_ago, 0), 1) >= p.retention_4y_cutoff
      then true else false
    end as badge_legacy_4y,
    case
      when b.amount_5y_ago >= p.holder_threshold
       and least(b.amount_now / nullif(b.amount_5y_ago, 0), 1) >= p.retention_5y_cutoff
      then true else false
    end as badge_legacy_5y
  from base b
  cross join params p
)

select
  dense_rank() over (
    order by
      c.final_score desc,
      c.weighted_score desc,
      case when c.badge_gate_70_60_50 then 1 else 0 end desc,
      case when c.badge_legacy_4y then 1 else 0 end desc,
      case when c.badge_legacy_5y then 1 else 0 end desc,
      c.amount_now desc
  ) as lth_rank,
  concat(
    case when c.amount_now >= p.whale_threshold then '🐋' else '' end,
    case
      when c.weighted_score >= c.weighted_cutoff then
        case
          when c.badge_gate_70_60_50 and c.badge_legacy_4y and c.badge_legacy_5y then '🪨🛡️4️⃣5️⃣'
          when c.badge_gate_70_60_50 and c.badge_legacy_4y then '🪨🛡️4️⃣'
          when c.badge_gate_70_60_50 and c.badge_legacy_5y then '🪨🛡️5️⃣'
          when c.badge_gate_70_60_50 then '🪨🛡️'
          when c.badge_legacy_4y and c.badge_legacy_5y then '🪨4️⃣5️⃣'
          when c.badge_legacy_4y then '💎4️⃣'
          when c.badge_legacy_5y then '💎5️⃣'
          else '💎'
        end
      else '❌'
    end
  ) as lth_tier,
  c.address,
  c.amount_now,
  c.amount_1y_ago,
  c.amount_2y_ago,
  c.amount_3y_ago,
  c.amount_4y_ago,
  c.amount_5y_ago,
  c.r1_cap as retention_ratio_1y,
  c.r2_cap as retention_ratio_2y,
  c.r3_cap as retention_ratio_3y,
  c.r4_cap as retention_ratio_4y,
  c.r5_cap as retention_ratio_5y,
  --c.weighted_score,
  (case when c.amount_now >=  p.holder_threshold then 1 else 0 end
   + case when c.amount_1y_ago >= p.holder_threshold then 1 else 0 end
   + case when c.amount_2y_ago >= p.holder_threshold then 1 else 0 end
   + case when c.amount_3y_ago >= p.holder_threshold then 1 else 0 end
   + case when c.amount_4y_ago >= p.holder_threshold then 1 else 0 end
   + case when c.amount_5y_ago >= p.holder_threshold then 1 else 0 end) as holding_years_count
from calc c
  cross join params p
where c.amount_now >= p.holder_threshold
  and c.amount_1y_ago >= p.holder_threshold
  and c.amount_2y_ago >= p.holder_threshold
  and c.amount_3y_ago >= p.holder_threshold
  and c.weighted_score >= p.weighted_cutoff
order by lth_rank
