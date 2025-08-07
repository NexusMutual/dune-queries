with lending_base_borrow as (

  with 

  src_LendingPool_evt_Borrow as (
    select * from aave_v3_ethereum.Pool_evt_Borrow
    where evt_block_time >= timestamp '2024-05-23'
      and coalesce(onBehalfOf, user) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ),

  src_LendingPool_evt_Repay as (
    select * from aave_v3_ethereum.Pool_evt_Repay
    where evt_block_time >= timestamp '2024-05-23'
      and user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ),

  src_LendingPool_evt_LiquidationCall as (
    select * from aave_v3_ethereum.Pool_evt_LiquidationCall
    where evt_block_time >= timestamp '2024-05-23'
      and user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ),

  base_borrow as (
    select
      'borrow' as transaction_type,
      case 
        when interestRateMode = 1 then 'stable'
        when interestRateMode = 2 then 'variable'
      end as loan_type,
      reserve as token_address,
      user as borrower,
      onBehalfOf as on_behalf_of,
      cast(null as varbinary) as repayer,
      cast(null as varbinary) as liquidator,
      cast(amount as double) as amount,
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from src_LendingPool_evt_Borrow
    union all
    select
      'repay' as transaction_type,
      null as loan_type,
      reserve as token_address,
      user as borrower,
      cast(null as varbinary) as on_behalf_of,
      repayer as repayer,
      cast(null as varbinary) as liquidator,
      -1 * cast(amount as double) as amount,
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from src_LendingPool_evt_Repay
    union all
    select
      'borrow_liquidation' as transaction_type,
      null as loan_type,
      debtAsset as token_address,
      user as borrower,
      cast(null as varbinary) as on_behalf_of,
      liquidator as repayer,
      liquidator as liquidator,
      -1 * cast(debtToCover as double) as amount,
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from src_LendingPool_evt_LiquidationCall
  )

  select
    transaction_type,
    loan_type,
    token_address,
    borrower,
    on_behalf_of,
    repayer,
    liquidator,
    amount,
    evt_block_time as block_time,
    evt_block_number as block_number,
    contract_address as project_contract_address,
    evt_tx_hash as tx_hash,
    evt_index
  from base_borrow
 
)

select
  borrow.block_time,
  borrow.block_number,
  borrow.transaction_type,
  borrow.loan_type,
  erc20.symbol,
  borrow.token_address,
  borrow.amount / power(10, coalesce(erc20.decimals, 18)) as amount,
  --borrow.amount / power(10, coalesce(p.decimals, erc20.decimals, 18)) * p.price as amount_usd,
  borrow.borrower,
  borrow.on_behalf_of,
  borrow.repayer,
  borrow.liquidator,
  borrow.project_contract_address,
  borrow.evt_index,
  borrow.tx_hash
from lending_base_borrow borrow
  inner join tokens.erc20 erc20
    on borrow.token_address = erc20.contract_address
    and erc20.blockchain = 'ethereum'
  /*inner join prices.usd p 
    on date_trunc('minute', borrow.block_time) = p.minute
    and borrow.token_address = p.contract_address
    and p.blockchain = 'ethereum'*/
