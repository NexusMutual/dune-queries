with

wallets as (
  select distinct cover_owner from nexusmutual_ethereum.covers_v2
)

select
  coalesce(on_behalf_of, borrower) as borrower,
  'borrow' as position_type,
  blockchain,
  project,
  version,
  symbol,
  sum(amount) as amount,
  sum(amount_usd) as amount_usd
from lending.borrow
where coalesce(on_behalf_of, borrower) in (select cover_owner from wallets)
group by 1,2,3,4,5,6
order by 1,2,3,4,5
--limit 10
