select
  cover_month as report_month,
  eth_premium,
  usd_premium,
  eth_claim_paid,
  usd_claim_paid,
  eth_member_fee,
  usd_member_fee
from query_4836553 -- cash surplus
where cover_month >= date_add('month', -12, current_date)
order by 1 desc
