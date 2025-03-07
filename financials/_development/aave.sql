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
  and coalesce(on_behalf_of, depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
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
  and coalesce(on_behalf_of, borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
  and version = '3'



with

prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_usdc_usd_price
  from query_4627588 -- Capital Pool - base root
),

aweth_withdrawals as (
  select
    date_trunc('month', block_time) as block_month,
    sum(amount) as aweth_eth,
    sum(amount_usd) as aweth_usd
  from aave_ethereum.supply
  where block_time >= timestamp '2024-05-23'
    and coalesce(on_behalf_of, depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and version = '3'
    and transaction_type = 'withdraw'
    and symbol = 'WETH'
  group by 1
),

usdc_debt_repayments as (
  select
    date_trunc('month', b.block_time) as block_month,
    sum(b.amount / p.avg_eth_usd_price) as usdc_debt_eth,
    sum(b.amount_usd) as usdc_debt_usd
  from aave_ethereum.borrow b
    inner join prices p on date_trunc('day', b.block_time) = p.block_date
  where b.block_time >= timestamp '2024-05-23'
    and coalesce(b.on_behalf_of, b.borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and b.version = '3'
    and b.transaction_type = 'repay'
    and b.symbol = 'USDC'
  group by 1
)

select * from usdc_debt_repayments
