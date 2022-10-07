select
  "day" as day,
  "amount_raw" * 1E-18 as maple_eth
from
  erc20.view_token_balances_daily
where
  "wallet_address" = '\x9164E822Db664A1B139F39Cc3eCC40aecd276b0F' and
  "token_address" = '\x1a066b0109545455bc771e49e6edef6303cb0a93' and
  day > '01/01/2022'