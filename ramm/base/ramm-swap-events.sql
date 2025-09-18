select
  evt_block_time as block_time,
  'below' as swap_type,
  member,
  'NXM' as token_in,
  'ETH' as token_out,
  nxmIn / 1e18 as amount_in,
  ethOut / 1e18 as amount_out,
  (ethOut / 1e18) / (nxmIn / 1e18) as swap_price,
  evt_tx_hash as tx_hash
from nexusmutual_ethereum.Ramm_evt_NxmSwappedForEth
union all
select
  evt_block_time as block_time,
  'above' as swap_type,
  member,
  'ETH' as token_in,
  'NXM' as token_out,
  ethIn / 1e18 as amount_in,
  nxmOut / 1e18 as amount_out,
  (ethIn / 1e18) / (nxmOut / 1e18) as swap_price,
  evt_tx_hash as tx_hash
from nexusmutual_ethereum.Ramm_evt_EthSwappedForNxm
