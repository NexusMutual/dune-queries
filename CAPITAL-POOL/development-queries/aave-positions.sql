with

current_market as (
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

supplied as (
  select
    block_date,
    symbol,
    sum(atoken_amount) as atoken_amount
  from (
    select
      date_trunc('day', s.evt_block_time) as block_date,
      t.symbol,
      s.amount / power(10, t.decimals) / u.liquidityIndex * power(10, 27) as atoken_amount
    from aave_v3_ethereum.Pool_evt_Supply s
      inner join tokens.erc20 t on t.contract_address = s.reserve and t.blockchain = 'ethereum'
      inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
         on u.evt_block_number = s.evt_block_number
        and u.evt_index < s.evt_index
        and u.evt_tx_hash = s.evt_tx_hash
        and u.reserve = s.reserve
    where s.evt_block_time >= timestamp '2024-05-23'
      and s.onBehalfOf = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ) t
  group by 1, 2
),

supply_withdrawn as (
  select
    block_date,
    symbol,
    sum(atoken_amount) as atoken_amount
  from (
    select
      date_trunc('day', w.evt_block_time) as block_date,
      t.symbol,
      w.amount / power(10, t.decimals) / u.liquidityIndex * power(10, 27) as atoken_amount
    from aave_v3_ethereum.Pool_evt_Withdraw w
      inner join tokens.erc20 t on t.contract_address = w.reserve and t.blockchain = 'ethereum'
      inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
         on u.evt_block_number = w.evt_block_number
        and u.evt_index < w.evt_index
        and u.evt_tx_hash = w.evt_tx_hash
        and u.reserve = w.reserve
    where w.evt_block_time >= timestamp '2024-05-23'
      and w.user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ) t
  group by 1, 2
),

supply_repaid as (
  select
    block_date,
    symbol,
    sum(atoken_amount) as atoken_amount
  from (
    select
      date_trunc('day', r.evt_block_time) as block_date,
      t.symbol,
      r.amount / power(10, t.decimals) / u.liquidityIndex * power(10, 27) as atoken_amount
    from aave_v3_ethereum.Pool_evt_Repay r
      inner join tokens.erc20 t on t.contract_address = r.reserve and t.blockchain = 'ethereum'
      inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
         on u.evt_block_number = r.evt_block_number
        and u.evt_index < r.evt_index
        and u.evt_tx_hash = r.evt_tx_hash
        and u.reserve = r.reserve
    where r.evt_block_time >= timestamp '2024-05-23'
      and r.user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
      and r.useATokens
  ) t
  group by 1, 2
),

supply_liquidated as (
  select
    block_date,
    symbol,
    sum(atoken_amount) as atoken_amount
  from (
    select
      date_trunc('day', l.evt_block_time) as block_date,
      t.symbol,
      cast(l.liquidatedCollateralAmount as double) / power(10, t.decimals) / u.liquidityIndex * power(10, 27) as atoken_amount
    from aave_v3_ethereum.Pool_evt_LiquidationCall l
      inner join tokens.erc20 t on t.contract_address = l.collateralasset and t.blockchain = 'ethereum'
      inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
         on u.evt_block_number = l.evt_block_number
        and u.evt_index < l.evt_index
        and u.evt_tx_hash = l.evt_tx_hash
        and u.reserve = l.collateralAsset
    where l.evt_block_time >= timestamp '2024-05-23'
      and l.user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ) t
  group by 1, 2
),

scaled_supplies as (
  select
    cm.block_date,
    cm.symbol,
    sum(
      s.atoken_amount - coalesce(sw.atoken_amount, 0) - coalesce(sr.atoken_amount, 0) - coalesce(sl.atoken_amount, 0)
    ) over (order by cm.block_date) * cm.liquidity_index / power(10, 27) as supplied_amount
  from current_market cm
    left join supplied s on cm.block_date = s.block_date and cm.symbol = s.symbol
    left join supply_withdrawn sw on cm.block_date = sw.block_date and cm.symbol = sw.symbol
    left join supply_repaid sr on cm.block_date = sr.block_date and cm.symbol = sr.symbol
    left join supply_liquidated sl on cm.block_date = sl.block_date and cm.symbol = sl.symbol
  where cm.symbol = 'WETH'
),

borrowed as (
  select
    block_date,
    symbol,
    sum(atoken_amount) as atoken_amount
  from (
    select
      date_trunc('day', b.evt_block_time) as block_date,
      t.symbol,
      b.amount / power(10, t.decimals) / u.variableBorrowIndex * power(10, 27) as atoken_amount
    from aave_v3_ethereum.Pool_evt_Borrow b
      inner join tokens.erc20 t on b.reserve = t.contract_address and t.blockchain = 'ethereum'
      inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
         on u.evt_block_number = b.evt_block_number
        and u.evt_index < b.evt_index
        and u.evt_tx_hash = b.evt_tx_hash
        and u.reserve = b.reserve
    where b.evt_block_time >= timestamp '2024-05-23'
      and b.onBehalfOf = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ) t
  group by 1, 2
),

borrow_repaid as (
  select
    block_date,
    symbol,
    sum(atoken_amount) as atoken_amount
  from (
    select
      date_trunc('day', r.evt_block_time) as block_date,
      t.symbol,
      r.amount / power(10, t.decimals) / u.variableBorrowIndex * power(10, 27) as atoken_amount
    from aave_v3_ethereum.Pool_evt_Repay r
      inner join tokens.erc20 t on t.contract_address = r.reserve and t.blockchain = 'ethereum'
      inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
         on u.evt_block_number = r.evt_block_number
        and u.evt_index < r.evt_index
        and u.evt_tx_hash = r.evt_tx_hash
        and u.reserve = r.reserve
    where r.evt_block_time >= timestamp '2024-05-23'
      and r.user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
      and r.useATokens
  ) t
  group by 1, 2
),

borrow_liquidated as (
  select
    block_date,
    symbol,
    sum(atoken_amount) as atoken_amount
  from (
    select
      date_trunc('day', l.evt_block_time) as block_date,
      t.symbol,
      cast(l.debtToCover as double) / power(10, t.decimals) / u.variableBorrowIndex * power(10, 27) as atoken_amount
    from aave_v3_ethereum.Pool_evt_LiquidationCall l
      inner join tokens.erc20 t on t.contract_address = l.collateralasset and t.blockchain = 'ethereum'
      inner join aave_v3_ethereum.Pool_evt_ReserveDataUpdated u
         on u.evt_block_number = l.evt_block_number
        and u.evt_index < l.evt_index
        and u.evt_tx_hash = l.evt_tx_hash
        and u.reserve = l.collateralAsset
    where l.evt_block_time >= timestamp '2024-05-23'
      and l.user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ) t
  group by 1, 2
),

scaled_borrows as (
  select
    cm.block_date,
    cm.symbol,
    sum(
      b.atoken_amount - coalesce(br.atoken_amount, 0) - coalesce(bl.atoken_amount, 0)
    ) over (order by cm.block_date) * cm.variable_borrow_index / power(10, 27) as borrowed_amount
  from current_market cm
    left join borrowed b on cm.block_date = b.block_date and cm.symbol = b.symbol
    left join borrow_repaid br on cm.block_date = br.block_date and cm.symbol = br.symbol
    left join borrow_liquidated bl on cm.block_date = bl.block_date and cm.symbol = bl.symbol
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
  left join scaled_supplies s on ds.block_date = s.block_date
  left join scaled_borrows b on ds.block_date = b.block_date
order by 1
