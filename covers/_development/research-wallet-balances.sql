with

wallets as (
  select distinct cover_owner from nexusmutual_ethereum.covers_v2
)

select
  blockchain,
  day,
  address,
  token_address,
  token_symbol,
  balance,
  balance_usd
from tokens_ethereum.balances_daily
where day /*>=*/ = date_add('day', -1, current_date)
  and address in (select cover_owner from wallets)
  and balance_usd >= 1 -- at least $1
