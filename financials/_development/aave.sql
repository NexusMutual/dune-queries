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

prices as (
  select
    block_date,
    avg_eth_usd_price,
    avg_usdc_usd_price
  from query_4627588 -- Capital Pool - base root
),

capital_pool as (
  select
    date_trunc('month', block_date) as block_month,
    capital_pool,
    lag(capital_pool, 1) over (order by block_date) as capital_pool_prev,
    eth,
    lag(eth, 1) over (order by block_date) as eth_prev,
    steth,
    lag(steth, 1) over (order by block_date) as steth_prev,
    reth,
    lag(reth, 1) over (order by block_date) as reth_prev,
    nxmty,
    lag(nxmty, 1) over (order by block_date) as nxmty_prev,
    aweth,
    lag(aweth, 1) over (order by block_date) as aweth_prev,
    debt_usdc,
    lag(debt_usdc, 1) over (order by block_date) as debt_usdc_prev
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

aweth_collateral as (
  select
    date_trunc('month', block_time) as block_month,
    sum(if(transaction_type = 'deposit', amount)) as aweth_deposit_eth,
    sum(if(transaction_type = 'deposit', amount_usd)) as aweth_deposit_usd,
    sum(if(transaction_type = 'withdraw', amount)) as aweth_withdraw_eth,
    sum(if(transaction_type = 'withdraw', amount_usd)) as aweth_withdraw_usd
  from aave_ethereum.supply
  where block_time >= timestamp '2024-05-23'
    and coalesce(on_behalf_of, depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and version = '3'
    and symbol = 'WETH'
  group by 1
),

usdc_debt as (
  select
    date_trunc('month', b.block_time) as block_month,
    sum(if(b.transaction_type = 'borrow', b.amount) / p.avg_eth_usd_price) as usdc_debt_borrow_eth,
    sum(if(b.transaction_type = 'borrow', b.amount_usd)) as usdc_debt_borrow_usd,
    -1 * sum(if(b.transaction_type = 'repay', b.amount) / p.avg_eth_usd_price) as usdc_debt_repay_eth,
    -1 * sum(if(b.transaction_type = 'repay', b.amount_usd)) as usdc_debt_repay_usd
  from aave_ethereum.borrow b
    inner join prices p on date_trunc('day', b.block_time) = p.block_date
  where b.block_time >= timestamp '2024-05-23'
    and coalesce(b.on_behalf_of, b.borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and b.version = '3'
    and b.symbol = 'USDC'
  group by 1
),

capital_pool_enriched as (
  select
    cp.block_month,
    cp.aweth,
    cp.aweth_prev,
    aave_c.aweth_deposit_eth,
    aave_c.aweth_withdraw_eth,
    cp.debt_usdc,
    cp.debt_usdc_prev,
    aave_d.usdc_debt_borrow_eth,
    aave_d.usdc_debt_repay_eth
  from capital_pool cp
    left join aweth_collateral aave_c on cp.block_month = aave_c.block_month
    left join usdc_debt aave_d on cp.block_month = aave_d.block_month
)

select * from capital_pool_enriched order by 1 desc
