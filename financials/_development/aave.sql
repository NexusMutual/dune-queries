-- deposit & withdraw
select
  block_time,
  transaction_type,
  symbol,
  amount,
  amount_usd,
  tx_hash
from aave_ethereum.supply
where block_time >= timestamp '2024-05-23'
  and coalesce(on_behalf_of, depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  and version = '3'

-- borrow & repay
select
  block_time,
  transaction_type,
  symbol,
  amount,
  amount_usd,
  tx_hash
from aave_ethereum.borrow
where block_time >= timestamp '2024-05-23'
  and coalesce(on_behalf_of, borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  and version = '3'
