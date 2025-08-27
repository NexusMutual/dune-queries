with lending_base_supply as (

  with 

  src_LendingPool_evt_Deposit as (
    select * from aave_v3_ethereum.Pool_evt_Supply
    where evt_block_time >= timestamp '2024-05-23'
      and coalesce(onBehalfOf, user) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ),

  src_LendingPool_evt_Withdraw as (
    select * from aave_v3_ethereum.Pool_evt_Withdraw
    where evt_block_time >= timestamp '2024-05-23'
      --and (user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e or user is null) -- not easy to apply on this level
  ),

  src_WrappedTokenGatewayV3_call_withdrawETH as (
    select * from aave_v3_ethereum.WrappedTokenGatewayV3_call_withdrawETH
    where call_block_time >= timestamp '2024-05-23'
      and call_success
      and cast("to" as varbinary) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ),

  src_LendingPool_evt_Repay as (
    select * from aave_v3_ethereum.Pool_evt_Repay
    where useATokens -- ref: https://github.com/duneanalytics/spellbook/issues/6417    
      and evt_block_time >= timestamp '2024-05-23'
      and user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ),

  src_ParaSwapRepayAdapter_evt_Bought as (
    select * from aave_ethereum.ParaSwapRepayAdapter_evt_Bought
    where evt_block_time >= timestamp '2024-05-23'
      and evt_tx_to = 0x51ad1265c8702c9e96ea61fe4088c2e22ed4418e
  ),

  src_LendingPool_evt_LiquidationCall as (
    select * from aave_v3_ethereum.Pool_evt_LiquidationCall
    where evt_block_time >= timestamp '2024-05-23'
      and user = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
  ),

  base_supply as (
    select
      'deposit' as transaction_type,
      d.reserve as token_address,
      d.user as depositor,
      d.onBehalfOf as on_behalf_of,
      cast(null as varbinary) as withdrawn_to,
      cast(null as varbinary) as liquidator,
      cast(d.amount as double) as amount,
      d.contract_address,
      d.evt_tx_hash,
      d.evt_index,
      d.evt_block_time,
      d.evt_block_number
    from src_LendingPool_evt_Deposit d
      left join src_ParaSwapRepayAdapter_evt_Bought pb
        on d.evt_block_number = pb.evt_block_number
        and d.evt_tx_hash = pb.evt_tx_hash
        and d.reserve = pb.fromAsset
    where pb.evt_block_number is null
    union all
    select
      'withdraw' as transaction_type,
      w.reserve as token_address,
      w.user as depositor,
      cast(wrap.to as varbinary) as on_behalf_of,
      w.to as withdrawn_to,
      cast(null as varbinary) as liquidator,
      -1 * cast(w.amount as double) as amount,
      w.contract_address,
      w.evt_tx_hash,
      w.evt_index,
      w.evt_block_time,
      w.evt_block_number
    from src_LendingPool_evt_Withdraw w
      left join src_WrappedTokenGatewayV3_call_withdrawETH wrap
        on w.evt_block_number = wrap.call_block_number
        and w.evt_tx_hash = wrap.call_tx_hash
        and w.to = wrap.contract_address
        --and w.amount = wrap.amount
        and wrap.call_success
    where coalesce(cast(wrap.to as varbinary), w.user) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e
    union all
    select
      'repay_with_atokens' as transaction_type,
      reserve as token_address,
      user as depositor,
      cast(null as varbinary) as on_behalf_of,
      repayer as withdrawn_to,
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
      'swap_and_repay' as transaction_type,
      fromAsset as token_address,
      evt_tx_from as depositor,
      evt_tx_to as on_behalf_of,
      cast(null as varbinary) as withdrawn_to,
      cast(null as varbinary) as liquidator,
      -1 * cast(amountSold as double) as amount,
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from src_ParaSwapRepayAdapter_evt_Bought pb
    union all
    select
      'deposit_liquidation' as transaction_type,
      collateralAsset as token_address,
      user as depositor,
      cast(null as varbinary) as on_behalf_of,
      liquidator as withdrawn_to,
      liquidator as liquidator,
      -1 * cast(liquidatedCollateralAmount as double) as amount,
      contract_address,
      evt_tx_hash,
      evt_index,
      evt_block_time,
      evt_block_number
    from src_LendingPool_evt_LiquidationCall
  )

  select
    transaction_type,
    token_address,
    depositor,
    on_behalf_of,
    withdrawn_to,
    liquidator,
    amount,
    evt_block_time as block_time,
    evt_block_number as block_number,
    contract_address as project_contract_address,
    evt_tx_hash as tx_hash,
    evt_index
  from base_supply

)

select
  '3' as version,
  supply.block_time,
  supply.block_number,
  supply.transaction_type,
  erc20.symbol,
  supply.token_address,
  supply.amount / power(10, coalesce(erc20.decimals, 18)) as amount,
  --supply.amount / power(10, coalesce(p.decimals, erc20.decimals, 18)) * p.price as amount_usd,
  supply.depositor,
  supply.on_behalf_of,
  supply.withdrawn_to,
  supply.liquidator,
  supply.project_contract_address,
  supply.evt_index,
  supply.tx_hash
from lending_base_supply supply
  inner join tokens.erc20 erc20
    on supply.token_address = erc20.contract_address
    and erc20.blockchain = 'ethereum'
  /*inner join prices.usd p 
    on date_trunc('minute', supply.block_time) = p.minute
    and supply.token_address = p.contract_address
    and p.blockchain = 'ethereum'*/
