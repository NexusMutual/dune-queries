/*
0xcafea7191d1B8538076feB019B092a53Dc4dCdFe
0xcafea19bB9ab657f8Ea74363C937446C0846a320
0xb00b58b77ECF669D6Cc5a8fc34783Bc244E3e045
0xcafea4E03B98873B842D83ed368F6F1A49F58Ee7
0xcafeaD1b3A9B57FE2A59Cd5D44de9BD54571b1BB
0xcafea5C050E74a21C11Af78C927e17853153097D
0xcafea3cA5366964A102388EAd5f3eBb0769C46Cb
0xcafeaed98d7Fce8F355C03c9F3507B90a974f37e
*/

select
  evt_block_time,
  fromAsset,
  toAsset,
  amountIn,
  amountOut,
  evt_tx_hash
from nexusmutual_ethereum.swapoperator_evt_swapped


--select distinct s.fromAsset, t.symbol
select distinct s.toAsset, t.symbol
from (
    select * from  nexusmutual_ethereum.swapoperator_evt_swapped
    union all
    select * from  nexusmutual_ethereum_ethereum.swapoperator_evt_swapped
  ) s
  left join tokens.erc20 t on s.toAsset = t.contract_address and t.blockchain = 'ethereum'



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



with

order_closed as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    filledAmount as filled_amount,
    from_hex(json_query("order", 'lax $.sellToken' omit quotes)) as sell_token,
    from_hex(json_query("order", 'lax $.buyToken' omit quotes)) as buy_token,
    from_hex(json_query("order", 'lax $.receiver' omit quotes)) as receiver,
    cast(json_query("order", 'lax $.sellAmount') as uint256) as sell_amount,
    cast(json_query("order", 'lax $.buyAmount') as uint256) as buy_amount,
    cast(json_query("order", 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query("order", 'lax $.partiallyFillable') as boolean) as partially_fillable,
    --"order",
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum_ethereum.swapoperator_evt_orderclosed
),

order_closed_ext as (
  select
    o.block_time,
    o.block_number,
    if(starts_with(st.symbol, 'W'), substr(st.symbol, 2), st.symbol) as sell_token_symbol,
    if(starts_with(bt.symbol, 'W'), substr(bt.symbol, 2), bt.symbol) as buy_token_symbol,
    o.receiver,
    o.sell_amount / power(10, st.decimals) as sell_amount,
    o.buy_amount / power(10, bt.decimals) as buy_amount,
    o.sell_amount as sell_amount_raw,
    o.buy_amount as buy_amount_raw,
    o.filled_amount as filled_amount_raw,
    o.fee_amount as fee_amount_raw,
    o.partially_fillable,
    o.tx_hash
  from order_closed o
    inner join tokens.erc20 st on o.sell_token = st.contract_address and st.blockchain = 'ethereum'
    inner join tokens.erc20 bt on o.buy_token = bt.contract_address and bt.blockchain = 'ethereum'
)

select
  o.block_time,
  case
    when o.filled_amount_raw = 0 and o.sell_token_symbol = t.symbol and o.sell_amount_raw = t.amount_raw then 'no fill'
    when o.filled_amount_raw > 0 and o.buy_token_symbol = t.symbol and o.buy_amount_raw <= t.amount_raw then 'full fill'
    when o.filled_amount_raw > 0 and o.buy_token_symbol = t.symbol and o.buy_amount_raw > t.amount_raw then 'partial fill'
    when o.filled_amount_raw > 0 and o.sell_token_symbol = t.symbol then 'funds returned'
  end as fill_type,
  o.receiver as swap_operator_contract,
  t.to as capital_pool_contract,
  o.sell_token_symbol,
  o.buy_token_symbol,
  o.sell_amount,
  o.buy_amount,
  t.amount as fill_amount,
  --o.filled_amount_raw,
  --o.fee_amount_raw,
  o.partially_fillable,
  o.tx_hash
from order_closed_ext o
  inner join tokens_ethereum.transfers t
    on t.block_time >= timestamp '2023-07-21'
    and o.block_time = t.block_time
    and o.block_number = t.block_number
    and o.tx_hash = t.tx_hash
    and o.receiver = t."from"
where ((o.filled_amount_raw = 0 and o.sell_token_symbol = t.symbol and o.sell_amount_raw = t.amount_raw) -- no fill
  or (o.filled_amount_raw > 0 and o.buy_token_symbol = t.symbol) -- full/partial fill
  or (o.filled_amount_raw > 0 and o.sell_token_symbol = t.symbol)) -- funds returned (on partial fill)
  and o.tx_hash = 0x2bd9f49b654ceb4dec91de102fec9e3eb7e2b9c041dfb00c347b5ce8ebad7e9d
order by 1 desc
