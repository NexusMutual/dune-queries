--select * from nexusmutual_ethereum.swap_order_placed order by 1 desc
--select * from nexusmutual_ethereum.swap_order_closed order by 1 desc

with

prices as (
  select
    block_date,
    avg_eth_usd_price
  from query_4627588 -- Capital Pool - base root
),

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
),

steth_sales_agg as (
  select
    date_trunc('month', s.block_date) as block_month,
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount)) as eth_sell_amount,
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount) * p.avg_eth_usd_price) as usd_sell_amount,
    sum(s.fill_amount) as eth_fill_amount,
    sum(s.fill_amount * p.avg_eth_usd_price) as usd_fill_amount,
    -1 * sum(coalesce(sa.sell_amount, s.sell_amount) - s.fill_amount) as eth_disposal_loss,
    -1 * sum((coalesce(sa.sell_amount, s.sell_amount) - s.fill_amount) * p.avg_eth_usd_price) as usd_disposal_loss
  from steth_sales s
    inner join prices p on s.block_date = p.block_date
    left join steth_sales_adjusted sa on sa.block_time = s.block_time and sa.tx_hash = s.tx_hash
  where s.fill_type <> 'funds returned'
  group by 1
)

select * from steth_sales_agg order by 1 desc

/*
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
*/
