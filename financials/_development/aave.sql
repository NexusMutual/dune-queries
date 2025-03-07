/*
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
*/

with

capital_pool as (
  select
    date_trunc('month', block_date) as block_month,
    capital_pool,
    lead(capital_pool, 1) over (order by block_date desc) as capital_pool_prev,
    eth,
    lead(eth, 1) over (order by block_date desc) as eth_prev,
    steth,
    lead(steth, 1) over (order by block_date desc) as steth_prev,
    reth,
    lead(reth, 1) over (order by block_date desc) as reth_prev,
    nxmty,
    lead(nxmty, 1) over (order by block_date desc) as nxmty_prev,
    aweth,
    lead(aweth, 1) over (order by block_date desc) as aweth_prev,
    debt_usdc,
    lead(debt_usdc, 1) over (order by block_date desc) as debt_usdc_prev
  from (
    select
      block_date,
      avg_capital_pool_eth_total as capital_pool,
      eth_total as eth,
      steth_total as steth,
      avg_reth_eth_total as reth,
      nxmty_eth_total as nxmty,
      aave_collateral_weth_total as aweth,
      avg_aave_debt_usdc_eth_total as debt_usdc,
      row_number() over (partition by date_trunc('month', block_date) order by block_date desc) as rn
    from query_4627588 -- Capital Pool - base root
  ) t
  where t.rn = 1
  order by 1 desc
  limit 12 -- 12 months rolling
),

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
    -1 * sum(b.amount / p.avg_eth_usd_price) as usdc_debt_eth,
    -1 * sum(b.amount_usd) as usdc_debt_usd
  from aave_ethereum.borrow b
    inner join prices p on date_trunc('day', b.block_time) = p.block_date
  where b.block_time >= timestamp '2024-05-23'
    and coalesce(b.on_behalf_of, b.borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and b.version = '3'
    and b.transaction_type = 'repay'
    and b.symbol = 'USDC'
  group by 1
),

capital_pool_aave_adjustments as (
  select
    cp.block_month,
    cp.capital_pool,
    cp.capital_pool_prev,
    cp.eth,
    cp.eth_prev,
    cp.steth,
    cp.steth_prev,
    cp.reth,
    cp.reth_prev,
    cp.nxmty,
    cp.nxmty_prev,
    cp.aweth,
    cp.aweth_prev,
    w.aweth_eth as aweth_withdrawal,
    cp.debt_usdc,
    cp.debt_usdc_prev,
    r.usdc_debt_eth as usdc_debt_repayment
  from capital_pool cp
    left join aweth_withdrawals w on cp.block_month = w.block_month
    left join usdc_debt_repayments r on cp.block_month = r.block_month
)

select * from capital_pool_aave_adjustments order by 1 desc
