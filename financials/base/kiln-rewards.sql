with

constants as (
  select 
    181.45 as kiln_first_round,
    194.7714625 as kiln_accrued_rewards,
    date '2023-08-30' as kiln_deposit_date,
    date '2024-04-24' as kiln_claim_start_date,
    date '2024-04-26' as kiln_claim_end_date,
    date '2025-04-15' as kiln_final_withdrawal_date
),

date_sequence as (
  select d.seq_date 
  from constants,
    unnest(sequence(kiln_deposit_date, kiln_final_withdrawal_date, interval '1' day)) as d(seq_date)
),

calc as (
  select 
    *,
    date_diff('day', kiln_claim_start_date, kiln_deposit_date) as days_first_kiln_rewards,
    date_diff('day', kiln_final_withdrawal_date, kiln_claim_end_date) as days_current_kiln_rewards
  from constants
),

rewards as (
  select 
    d.seq_date,
    case 
      when d.seq_date <= c.kiln_claim_start_date
        then c.kiln_first_round / c.days_first_kiln_rewards * date_diff('day', d.seq_date, c.kiln_deposit_date)
      when d.seq_date >= c.kiln_claim_end_date
        then c.kiln_accrued_rewards / c.days_current_kiln_rewards * date_diff('day', d.seq_date, c.kiln_claim_end_date)
      else 0 
    end as kiln_rewards
  from date_sequence d, calc c
)

select seq_date, greatest(kiln_rewards, 0) as kiln_rewards
from rewards
order by 1 desc
