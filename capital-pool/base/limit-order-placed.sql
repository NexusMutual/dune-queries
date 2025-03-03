with

order_placed as (
  select
    eo.evt_block_time as block_time,
    eo.evt_block_number as block_number,
    co.orderUID as order_id,
    from_hex(json_query(eo."order", 'lax $.sellToken' omit quotes)) as sell_token,
    from_hex(json_query(eo."order", 'lax $.buyToken' omit quotes)) as buy_token,
    from_hex(json_query(eo."order", 'lax $.receiver' omit quotes)) as receiver,
    cast(json_query(eo."order", 'lax $.sellAmount') as uint256) as sell_amount,
    cast(json_query(eo."order", 'lax $.buyAmount') as uint256) as buy_amount,
    cast(json_query(eo."order", 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query(eo."order", 'lax $.partiallyFillable') as boolean) as partially_fillable,
    --eo."order",
    eo.evt_tx_hash as tx_hash
  from nexusmutual_ethereum_ethereum.swapoperator_evt_orderplaced eo
    inner join nexusmutual_ethereum_ethereum.swapoperator_call_placeorder co on eo.evt_block_time = co.call_block_time and eo.evt_tx_hash = co.call_tx_hash
  where co.call_success
),

order_placed_ext as (
  select
    o.block_time,
    o.block_number,
    o.order_id,
    if(starts_with(st.symbol, 'W'), substr(st.symbol, 2), st.symbol) as sell_token_symbol,
    if(starts_with(bt.symbol, 'W'), substr(bt.symbol, 2), bt.symbol) as buy_token_symbol,
    o.receiver,
    o.sell_amount / power(10, st.decimals) as sell_amount,
    o.buy_amount / power(10, bt.decimals) as buy_amount,
    o.sell_amount as sell_amount_raw,
    o.buy_amount as buy_amount_raw,
    o.fee_amount as fee_amount_raw,
    o.partially_fillable,
    o.tx_hash
  from order_placed o
    inner join tokens.erc20 st on o.sell_token = st.contract_address and st.blockchain = 'ethereum'
    inner join tokens.erc20 bt on o.buy_token = bt.contract_address and bt.blockchain = 'ethereum'
)

select
  o.block_time,
  o.block_number,
  t."from" as capital_pool_contract,
  o.receiver as  swap_operator_contract,
  o.sell_token_symbol,
  o.buy_token_symbol,
  o.sell_amount,
  o.buy_amount,
  o.fee_amount_raw,
  o.partially_fillable,
  o.order_id,
  o.tx_hash
from order_placed_ext o
  inner join tokens_ethereum.transfers t
    on t.block_time >= timestamp '2023-07-21'
    and o.block_time = t.block_time
    and o.block_number = t.block_number
    and o.tx_hash = t.tx_hash
    and o.receiver = t.to
    and o.sell_token_symbol = t.symbol
    --and o.sell_amount_raw = t.amount_raw
order by 1 desc
