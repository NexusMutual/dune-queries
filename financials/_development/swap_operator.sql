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
from nexusmutual_ethereum.swapoperator_evt_swapped s
  left join tokens.erc20 t on s.toAsset = t.contract_address and t.blockchain = 'ethereum'
