with aave_supply as (
  select
    --date_trunc('month', block_time) as block_month,
    sum(if(transaction_type = 'deposit', amount)) as eth_aweth_deposit,
    sum(if(transaction_type in ('withdraw', 'swap_and_repay'), abs(amount))) as eth_aweth_withdraw
  from query_5595175 -- aave supply - base
  where block_time >= timestamp '2024-05-23'
    and coalesce(on_behalf_of, depositor) = 0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e -- Advisory Board multisig
    and version = '3'
    and symbol = 'WETH'
  --group by 1
)

select eth_aweth_deposit, eth_aweth_withdraw, eth_aweth_withdraw - eth_aweth_deposit as eth_aweth_interest
from aave_supply
