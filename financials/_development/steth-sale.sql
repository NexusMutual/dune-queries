--select * from nexusmutual_ethereum.swap_order_placed order by 1 desc
--select * from nexusmutual_ethereum.swap_order_closed order by 1 desc

with

steth_sales as (
  select
    block_time,
    block_date,
    fill_type,
    sell_amount,
    buy_amount,
    fill_amount,
    tx_hash
  from nexusmutual_ethereum.swap_order_closed
  where sell_token_symbol = 'stETH'
    and buy_token_symbol = 'ETH'
    and fill_type <> 'no fill'
),

steth_sales_adjusted as (
  select
    block_time,
    block_date,
    fill_type,
    sell_amount - fill_amount as sell_amount,
    buy_amount,
    fill_amount,
    tx_hash
  from steth_sales
  where fill_type = 'funds returned'
)

select
  s.block_time,
  s.block_date,
  s.fill_type,
  coalesce(sa.sell_amount, s.sell_amount) as sell_amount,
  s.buy_amount,
  s.fill_amount,
  coalesce(sa.sell_amount, s.sell_amount) - s.fill_amount as disposal_loss,
  s.tx_hash
from steth_sales s
  left join steth_sales_adjusted sa on sa.block_time = s.block_time and sa.tx_hash = s.tx_hash
where s.fill_type <> 'funds returned'
