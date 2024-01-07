WITH
    non_extended_tranche_deposits AS (
    SELECT
      *
    FROM
      nexusmutual_ethereum.StakingPool_call_depositTo
    WHERE
      output_tokenId NOT IN (
        SELECT
          tokenId
        FROM
          nexusmutual_ethereum.StakingPool_call_extendDeposit
      )
      AND call_success
      AND contract_address != 0xcafeacf62fb96fa1243618c4727edf7e04d1d4ca
  ),
  deposits_with_extentions AS (
    SELECT
      a.call_block_time as extension_start_time,
      amount * 1e-18 as original_deposit,
      topUpAmount * 1e-18 as deposit_topup_amount,
      initialTrancheId as initial_tranche_id,
      newTrancheId AS extended_tranche_id,
      tokenId AS nft_id,
      a.call_tx_hash as extention_call_tx_hash,
      b.call_block_time as deposit_start_time,
      b.call_tx_hash as deposit_call_tx_hash,
      b.call_trace_address AS deposit_call_trace_address
    FROM
      nexusmutual_ethereum.StakingPool_call_extendDeposit AS a
      LEFT JOIN nexusmutual_ethereum.StakingPool_call_depositTo AS b on --ON a.initialTrancheId = b.trancheId
      a.tokenId = b.output_tokenId
      AND b.contract_address != 0xcafeacf62fb96fa1243618c4727edf7e04d1d4ca
  ),
  -- Users can sequently extend a staking NFT and add nxm as they go, the tops have to be summed over the lifetime of the nft to be able to track
  cumulative_extensions_on_single_nft AS (
    SELECT
      *,
      RANK() OVER (
        PARTITION BY
          nft_id
        ORDER BY
          extension_start_time ASC
      ) AS start_time_rank,
      SUM(deposit_topup_amount) OVER (
        PARTITION BY
          nft_id
        ORDER BY
          extension_start_time ASC
      ) AS summed_topup
    FROM
      deposits_with_extentions
  ),
    tranche_deposits AS (
    SELECT
      call_block_time AS ts,
      call_tx_hash,
      call_trace_address,
      CAST(amount AS DOUBLE) * 1E-18 AS staked_amount
    FROM
      non_extended_tranche_deposits
    UNION ALL
    SELECT
      from_unixtime(91.0 * 86400.0 * CAST(trancheId + 1 AS DOUBLE)) AS ts,
      call_tx_hash,
      call_trace_address,
      CAST(amount AS DOUBLE) * -1E-18 AS staked_amount
    FROM
      non_extended_tranche_deposits
    UNION ALL
    SELECT
      extension_start_time AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit + summed_topup as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    UNION ALL
    SELECT
      from_unixtime(
        91.0 * 86400.0 * CAST(extended_tranche_id + 1 AS DOUBLE)
      ) AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      (original_deposit + summed_topup) * -1 as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    UNION ALL
    SELECT
      deposit_start_time AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit * 1 as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    WHERE
      start_time_rank = 1
    UNION ALL -- initial deposit
    SELECT
      extension_start_time AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit * -1 as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    WHERE
      start_time_rank = 1
    UNION ALL
    SELECT
      a.extension_start_time as ts,
      a.deposit_call_tx_hash AS call_tx_hash,
      a.deposit_call_trace_address AS call_trace_address,
      (b.summed_topup + b.original_deposit) * -1.0 as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft AS a
      INNER JOIN cumulative_extensions_on_single_nft AS b ON a.nft_id = b.nft_id
      AND a.initial_tranche_id = b.extended_tranche_id
    UNION ALL
    SELECT
      from_unixtime(
        91.0 * 86400.0 * CAST(a.initial_tranche_id + 1 AS DOUBLE)
      ) AS ts,
      a.deposit_call_tx_hash AS call_tx_hash,
      a.deposit_call_trace_address AS call_trace_address,
      (b.summed_topup + b.original_deposit) * 1.0 as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft AS a
      INNER JOIN cumulative_extensions_on_single_nft AS b ON a.nft_id = b.nft_id
      AND a.initial_tranche_id = b.extended_tranche_id
  ),
  active_deposits AS (
    SELECT
      *
    FROM
      tranche_deposits
    WHERE
      ts <= NOW()
  ),
  ranked_pool_managers AS (
    SELECT
      manager AS manager_address,
      poolId AS pool_id,
      RANK() OVER (
        PARTITION BY
          poolId
        ORDER BY
          call_block_time,
          call_trace_address DESC
      ) AS ranked
    FROM
      nexusmutual_ethereum.TokenController_call_assignStakingPoolManager
  ),
  staker AS (
    SELECT
      COALESCE(manager_address, "from") AS staker,
      manager_address AS staker_manager,
      a.call_tx_hash,
      a.call_trace_address,
      a.poolId as pool_id
    FROM
      nexusmutual_ethereum.TokenController_call_depositStakedNXM AS a
      LEFT JOIN ranked_pool_managers AS b ON "from" = 0x84edffa16bb0b9ab1163abb0a13ff0744c11272f
      AND b.pool_id = a.poolId
      AND ranked = 1
  ),
  active_deposited_by_pool AS (
    SELECT
      *
    FROM
      active_deposits AS a
      LEFT JOIN staker AS b ON a.call_tx_hash = b.call_tx_hash
      AND SLICE(
        b.call_trace_address,
        1,
        cardinality(a.call_trace_address)
      ) = a.call_trace_address
  ),
  net_staked AS (
    SELECT DISTINCT
      staker,
      SUM(CAST(staked_amount AS DOUBLE)) OVER (
        PARTITION BY
          staker
      ) AS net_nxm_staked,
      SUM(CAST(staked_amount AS DOUBLE)) OVER () AS net_nxm_staked_total
    FROM
      active_deposited_by_pool
  )
SELECT
  *
FROM
  net_staked
WHERE
  staker != 0x84edffa16bb0b9ab1163abb0a13ff0744c11272f