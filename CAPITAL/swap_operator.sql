select
  evt_block_time as date,
  case
    when "fromAsset" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' then -1 * "amountIn" * 1E-18
    when "toAsset" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' then "amountOut" * 1E-18
    else 0
  end as eth_change,
  case
    when "fromAsset" = '\x6b175474e89094c44da98b954eedeac495271d0f' then -1 * "amountIn"
    when "toAsset" = '\x6b175474e89094c44da98b954eedeac495271d0f' then "amountOut"
    else 0
  end as dai_change,
  case
    when "fromAsset" = '\xdfe66b14d37c77f4e9b180ceb433d1b164f0281d' then -1 * "amountIn"
    when "toAsset" = '\xdfe66b14d37c77f4e9b180ceb433d1b164f0281d' then "amountOut"
    else 0
  end as steth_change
from
  nexusmutual."SwapOperator_evt_Swapped"


0x775116496f2e8fee8d6a425f1327e48aabec035b
  select
    *,
    amount_raw * 1E-18 as amount
from
  erc20.token_balances
where
  wallet_address = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
  and token_address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'




  //enzyme nexus vault


  0x27F23c710dD3d878FE9393d93465FeD1302f2EbD