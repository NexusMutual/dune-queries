 SELECT
  *,
  amount * 1E-18 AS staked_amount,
  to_unixtime(call_block_time) / (91 * 86400) as starting_tranche,
  91 * 86400 * trancheId AS stake_expiry
FROM
  nexusmutual_ethereum.StakingPool_call_depositTo