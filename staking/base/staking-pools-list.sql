select
  evt_block_time as block_time,
  cast(poolId as int) as pool_id,
  stakingPoolAddress as pool_address,
  evt_tx_hash as tx_hash
from nexusmutual_ethereum.StakingPoolFactory_evt_StakingPoolCreated
