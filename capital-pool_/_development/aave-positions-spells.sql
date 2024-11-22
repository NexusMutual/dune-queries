with

aave_current_market as (
  select block_time, block_date, symbol, reserve, liquidity_index, variable_borrow_index
  from (
    select
      r.evt_block_time as block_time,
      date_trunc('day', r.evt_block_time) as block_date,
      t.symbol,
      r.reserve,
      r.liquidityIndex as liquidity_index,
      r.variableBorrowIndex as variable_borrow_index,
      row_number() over (partition by date_trunc('day', r.evt_block_time), r.reserve order by r.evt_block_number desc, r.evt_index desc) as rn
    from aave_v3_ethereum.Pool_evt_ReserveDataUpdated r
      inner join tokens.erc20 as t on r.reserve = t.contract_address and t.blockchain = 'ethereum'
    where r.evt_block_time >= timestamp '2024-05-23'
      and t.symbol in ('WETH', 'USDC')
  ) t
  where rn = 1
),

aave_supplied as (
  select
    date_trunc('day', s.block_time) as block_date,
    s.symbol,
    sum(s.amount / u.liquidityIndex * power(10, 27)) as atoken_amount
  from aave_ethereum.supply s
    inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
        on u.evt_block_number = s.block_number
      and u.evt_index < s.evt_index
      and u.evt_tx_hash = s.tx_hash
      and u.reserve = s.token_address
  where s.block_time >= timestamp '2024-05-23'
    and coalesce(s.on_behalf_of, s.depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
    and s.version = '3'
  group by 1, 2
),

aave_scaled_supplies as (
  select
    cm.block_date,
    cm.symbol,
    sum(s.atoken_amount) over (order by cm.block_date) * cm.liquidity_index / power(10, 27) as supplied_amount
  from aave_current_market cm
    left join aave_supplied s on cm.block_date = s.block_date and cm.symbol = s.symbol
  where cm.symbol = 'WETH'
),

aave_borrowed as (
  select
    date_trunc('day', b.block_time) as block_date,
    b.symbol,
    sum(b.amount / u.variableBorrowIndex * power(10, 27)) as atoken_amount
  from aave_ethereum.borrow b
    inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
        on u.evt_block_number = b.block_number
      and u.evt_index < b.evt_index
      and u.evt_tx_hash = b.tx_hash
      and u.reserve = b.token_address
  where b.block_time >= timestamp '2024-05-23'
    and coalesce(b.on_behalf_of, b.borrower) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
    and b.version = '3'
  group by 1, 2
),

aave_scaled_borrows as (
  select
    cm.block_date,
    cm.symbol,
    sum(b.atoken_amount) over (order by cm.block_date) * cm.variable_borrow_index / power(10, 27) as borrowed_amount
  from aave_current_market cm
    left join aave_borrowed b on cm.block_date = b.block_date and cm.symbol = b.symbol
  where cm.symbol = 'USDC'
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2024-05-23', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
)

select
  ds.block_date,
  s.supplied_amount as aave_weth_collateral,
  b.borrowed_amount as aave_usdc_debt
from day_sequence ds
  left join aave_scaled_supplies s on ds.block_date = s.block_date
  left join aave_scaled_borrows b on ds.block_date = b.block_date
order by 1
