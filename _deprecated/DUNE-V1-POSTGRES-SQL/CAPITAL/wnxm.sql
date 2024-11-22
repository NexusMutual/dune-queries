WITH
  erc_wnxm_transactions as (
    select
      "from" as address_from,
      "to" as address_to,
      value * 1E-18 as gross
    from
      erc20."ERC20_evt_Transfer"
    where
      contract_address = '\x0d438F3b5175Bebc262bF23753C1E53d03432bDE'
      and value > 0
  ),
  trans as (
    select
      address_to as address,
      gross
    from
      erc_wnxm_transactions
    UNION
    select
      address_from as address,
      -1 * gross
    from
      erc_wnxm_transactions
  )
select
DISTINCT 
  address,
  SUM(gross) OVER (PARTITION BY address)
from
  trans