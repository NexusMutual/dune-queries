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

investment_returns as (
  select
    block_date,
    capital_pool,
    capital_pool_prev,
    capital_pool - capital_pool_prev as capital_pool_return,
    coalesce((capital_pool - capital_pool_prev) / nullif(capital_pool_prev, 0), 0) as capital_pool_pct,
    coalesce(power(1 + ((capital_pool - capital_pool_prev) / nullif(capital_pool_prev, 0)), 12) - 1, 0) as capital_pool_apy,
    eth,
    eth_prev,
    eth - eth_prev as eth_return,
    coalesce((eth - eth_prev) / nullif(eth_prev, 0), 0) as eth_pct,
    coalesce(power(1 + ((eth - eth_prev) / nullif(eth_prev, 0)), 12) - 1, 0) as eth_apy,
    steth,
    steth_prev,
    steth - steth_prev as steth_return,
    coalesce((steth - steth_prev) / nullif(steth_prev, 0), 0) as steth_pct,
    coalesce(power(1 + ((steth - steth_prev) / nullif(steth_prev, 0)), 12) - 1, 0) as steth_apy,
    reth,
    reth_prev,
    reth - reth_prev as reth_return,
    coalesce((reth - reth_prev) / nullif(reth_prev, 0), 0) as reth_pct,
    coalesce(power(1 + ((reth - reth_prev) / nullif(reth_prev, 0)), 12) - 1, 0) as reth_apy,
    nxmty,
    nxmty_prev,
    nxmty - nxmty_prev as nxmty_return,
    coalesce((nxmty - nxmty_prev) / nullif(nxmty_prev, 0), 0) as nxmty_pct,
    coalesce(power(1 + ((nxmty - nxmty_prev) / nullif(nxmty_prev, 0)), 12) - 1, 0) as nxmty_apy,
    aweth,
    aweth_prev,
    aweth - aweth_prev as aweth_return,
    coalesce((aweth - aweth_prev) / nullif(aweth_prev, 0), 0) as aweth_pct,
    coalesce(power(1 + ((aweth - aweth_prev) / nullif(aweth_prev, 0)), 12) - 1, 0) as aweth_apy,
    debt_usdc,
    debt_usdc_prev,
    debt_usdc - debt_usdc_prev as debt_usdc_return,
    coalesce((debt_usdc - debt_usdc_prev) / nullif(debt_usdc_prev, 0), 0) as debt_usdc_pct,
    coalesce(power(1 + ((debt_usdc - debt_usdc_prev) / nullif(debt_usdc_prev, 0)), 12) - 1, 0) as debt_usdc_apy
  from capital_pool
)

select
  block_date,
  -- capital pool total
  capital_pool,
  capital_pool_return,
  capital_pool_pct, -- taking pct change instead of apy
  -- aave net return
  aweth_return + debt_usdc_return as aave_net,
  'xx' as aave_net_return,
  coalesce(power(1 + ((aweth_return + debt_usdc_return) / nullif(aweth_prev, 0)), 12) - 1, 0) as aweth_net_apy,
  -- eth investment returns
  'xx' as eth_inv,
  steth_return + reth_return + nxmty * (1-0.0015) + (aweth_return + debt_usdc_return) as eth_inv_returns,
  coalesce(power(1 + ((steth_return + reth_return + nxmty * (1-0.0015) + (aweth_return + debt_usdc_return))
   / nullif(((capital_pool_prev + capital_pool) / 2), 0)), 12) - 1, 0) as eth_inv_apy,
  -- investments
  eth,
  eth_return,
  eth_apy,
  steth,
  steth_return,
  steth_apy,
  reth,
  reth_return,
  reth_apy,
  nxmty * (1-0.0015) as nxmty, -- minus Enxyme fee
  nxmty_return,
  nxmty_apy,
  aweth,
  aweth_return,
  aweth_apy,
  debt_usdc,
  debt_usdc_return,
  debt_usdc_apy
from investment_returns
order by 1 desc
