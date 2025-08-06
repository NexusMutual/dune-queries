with

long_term_holders as (
  select
    address,
    current_balance,
    balance_2_years_ago,
    long_term_holder,
    adjusted_balance,
    adjusted_balance_ratio
  from dune.nexus_mutual.result_long_term_holders
),

btc_cover_buyers as (
  select
    cover_owner as address,
    cover_buys,
    total_cover_amount,
    total_cover_amount_ratio
  from dune.nexus_mutual.result_btc_cover_buyers
)

select
  coalesce(lth.address, bcb.address) as address,
  case when lth.address is not null then 'long term holder' else 'btc cover buyer' end as candidate_type,
  lth.current_balance,
  lth.balance_2_years_ago,
  lth.long_term_holder,
  lth.adjusted_balance,
  lth.adjusted_balance_ratio,
  bcb.cover_buys,
  bcb.total_cover_amount,
  bcb.total_cover_amount_ratio,
  coalesce(lth.adjusted_balance_ratio, 0) + coalesce(bcb.total_cover_amount_ratio, 0) as total_ratio
from long_term_holders lth
  full outer join btc_cover_buyers bcb on lth.address = bcb.address
order by total_ratio desc, candidate_type
