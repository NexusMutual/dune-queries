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
    evt_block_time as block_time,
    evt_block_number as block_number,
    from_hex(json_query("order", 'lax $.sellToken' omit quotes)) as sell_token,
    from_hex(json_query("order", 'lax $.buyToken' omit quotes)) as buy_token,
    from_hex(json_query("order", 'lax $.receiver' omit quotes)) as receiver,
    cast(json_query("order", 'lax $.sellAmount') as uint256) as sell_amount,
    cast(json_query("order", 'lax $.buyAmount') as uint256) as buy_amount,
    cast(json_query("order", 'lax $.feeAmount') as uint256) as fee_amount,
    cast(json_query("order", 'lax $.partiallyFillable') as boolean) as partially_fillable,
    --"order",
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum_ethereum.swapoperator_evt_orderplaced
),

order_placed_ext as (
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
    o.fee_amount as fee_amount_raw,
    o.partially_fillable,
    o.tx_hash
  from order_placed o
    left join tokens.erc20 st on o.sell_token = st.contract_address and st.blockchain = 'ethereum'
    left join tokens.erc20 bt on o.buy_token = bt.contract_address and bt.blockchain = 'ethereum'
)

select
  o.block_time,
  o.sell_token_symbol,
  o.buy_token_symbol,
  t."from" as sender, -- capital pool contract
  o.receiver, -- swap operator contract
  o.sell_amount,
  o.buy_amount,
  o.fee_amount_raw,
  o.partially_fillable,
  o.tx_hash
from order_placed_ext o
  left join tokens_ethereum.transfers t
    on t.block_time >= timestamp '2023-07-21'
    and o.block_time = t.block_time
    and o.block_number = t.block_number
    and o.tx_hash = t.tx_hash
    and o.receiver = t.to
    and o.sell_token_symbol = t.symbol
    and o.sell_amount_raw = t.amount_raw
order by 1 desc



select
  evt_block_time as block_time,
  filledAmount as filled_amount,
  t.symbol,
  t.amount,
  from_hex(json_query("order", 'lax $.sellToken' omit quotes)) as sell_token,
  from_hex(json_query("order", 'lax $.buyToken' omit quotes)) as buy_token,
  from_hex(json_query("order", 'lax $.receiver' omit quotes)) as receiver,
  cast(json_query("order", 'lax $.sellAmount') as uint256) as sell_amount,
  cast(json_query("order", 'lax $.buyAmount') as uint256) as buy_amount,
  cast(json_query("order", 'lax $.feeAmount') as uint256) as fee_amount,
  cast(json_query("order", 'lax $.partiallyFillable') as boolean) as partially_fillable,
  --"order",
  evt_tx_hash as tx_hash
from nexusmutual_ethereum_ethereum.swapoperator_evt_orderclosed o
  left join tokens_ethereum.transfers t on o.evt_block_time = t.block_time and o.evt_tx_hash = t.tx_hash
