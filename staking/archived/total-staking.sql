WITH
  v1_nxm_staking_transactions AS (
    SELECT DISTINCT
      DATE_TRUNC('day', evt_block_time) AS day,
      SUM(
        CASE
          WHEN "to" = 0x84EdfFA16bb0b9Ab1163abb0a13Ff0744c11272f THEN CAST(value AS DOUBLE) * 1E-18
          WHEN "from" = 0x84EdfFA16bb0b9Ab1163abb0a13Ff0744c11272f THEN CAST(value AS DOUBLE) * -1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          DATE_TRUNC('day', evt_block_time)
      ) AS amount
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      (
        "to" = 0x84EdfFA16bb0b9Ab1163abb0a13Ff0744c11272f
        OR "from" = 0x84EdfFA16bb0b9Ab1163abb0a13Ff0744c11272f
      )
  ),
  v1_staked_union AS (
    SELECT DISTINCT
      DATE_TRUNC('day', evt_block_time) AS ts,
      SUM(CAST(amount AS DOUBLE) * 1E-18) OVER (
        PARTITION BY
          DATE_TRUNC('day', evt_block_time)
      ) AS staked_amount
    FROM
      nexusmutual_ethereum.PooledStaking_evt_Staked
    UNION ALL
    SELECT DISTINCT
      DATE_TRUNC('day', evt_block_time) AS ts,
      SUM(CAST(amount AS DOUBLE) * -1E-18) OVER (
        PARTITION BY
          DATE_TRUNC('day', evt_block_time)
      ) AS staked_amount
    FROM
      nexusmutual_ethereum.PooledStaking_evt_Unstaked
  ),
  v1_staked AS (
    SELECT DISTINCT
      ts,
      SUM(staked_amount) OVER (
        PARTITION BY
          ts
      ) as staked_amount
    FROM
      v1_staked_union
  ),
  created_staking_pools AS (
    SELECT
      call_block_time,
      call_block_time as create_pool_ts,
      date_diff('day', call_block_time, NOW()) AS create_pool_days,
      output_0 AS pool_id,
      output_1 AS pool_address,
      maxPoolFee,
      isPrivatePool,
      json,
      CAST(JSON_QUERY(json, 'lax $.productId') AS INT) AS product_id,
      CAST(JSON_QUERY(json, 'lax $.weight') AS INT) AS weight,
      CAST(JSON_QUERY(json, 'lax $.initialPrice') AS INT) AS initial_price,
      CAST(JSON_QUERY(json, 'lax $.targetPrice') AS INT) AS target_price
    from
      nexusmutual_ethereum.Cover_call_createStakingPool
      LEFT JOIN UNNEST (productInitParams) AS t (json) ON TRUE
    WHERE
      call_success
      AND contract_address = 0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62
  ),
  staking_pool_updates AS (
    SELECT
      call_block_time,
      poolId AS pool_id,
      CAST(JSON_QUERY(json, 'lax $.productId') AS INT) AS product_id,
      CAST(
        JSON_QUERY(json, 'lax $.recalculateEffectiveWeight') AS BOOLEAN
      ) AS re_eval_eff_weight,
      CAST(
        JSON_QUERY(json, 'lax $.setTargetWeight') AS BOOLEAN
      ) AS set_target_weight,
      CAST(JSON_QUERY(json, 'lax $.targetWeight') AS DOUBLE) AS target_weight,
      CAST(
        JSON_QUERY(json, 'lax $.setTargetPrice') AS BOOLEAN
      ) AS set_target_price,
      CAST(JSON_QUERY(json, 'lax $.targetPrice') AS DOUBLE) AS target_price
    FROM
      nexusmutual_ethereum.StakingProducts_call_setProducts
      LEFT JOIN UNNEST (params) AS t (json) ON TRUE
    WHERE
      call_success
      AND contract_address = 0xcafea573fBd815B5f59e8049E71E554bde3477E4
      AND CAST(
        JSON_QUERY(json, 'lax $.setTargetWeight') AS BOOLEAN
      ) = true
  ),
  v2_product_allocated AS (
    SELECT
      call_block_time,
      pool_id,
      product_id,
      target_weight,
      target_price
    FROM
      staking_pool_updates
    UNION ALL
    SELECT
      call_block_time,
      pool_id,
      product_id,
      weight as target_weight,
      target_price
    FROM
      created_staking_pools
  ),
  ranked_product_allocations AS (
    SELECT
      *,
      RANK() OVER (
        PARTITION BY
          product_id,
          pool_id
        ORDER BY
          call_block_time ASC
      ) AS rank_entry,
      COUNT() OVER (
        PARTITION BY
          product_id,
          pool_id
      ) AS count_entry
    FROM
      v2_product_allocated
  ),
  product_allocation_over_time AS (
    SELECT DISTINCT
      s.call_block_time AS start_time,
      CASE
        WHEN (
          (s.rank_entry = s.count_entry)
          AND (t.rank_entry = t.count_entry)
        ) THEN NOW()
        ELSE t.call_block_time
      END AS finish_time,
      s.pool_id,
      s.product_id,
      s.target_weight AS weight,
      s.rank_entry AS s_rank_entry,
      COALESCE(t.rank_entry, 0) AS t_rank_entry,
      s.count_entry As s_count_entry,
      COALESCE(t.count_entry, 0) AS t_count_entry
    FROM
      ranked_product_allocations AS s
      INNER JOIN ranked_product_allocations AS t ON t.product_id = s.product_id
      AND (t.pool_id = s.pool_id)
      AND (
        (t.rank_entry = s.rank_entry + 1)
        OR (
          (s.rank_entry = s.count_entry)
          AND (t.rank_entry = t.count_entry)
        )
      )
  ),
  -- Get all staking deposits that have not been
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
  original_extended_tranche_deposits AS (
    SELECT
      *
    FROM
      nexusmutual_ethereum.StakingPool_call_depositTo
    WHERE
      output_tokenId IN (
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
      LEFT JOIN nexusmutual_ethereum.StakingPool_call_depositTo AS b ON a.initialTrancheId = b.trancheId
      AND a.tokenId = b.output_tokenId
      AND b.contract_address != 0xcafeacf62fb96fa1243618c4727edf7e04d1d4ca
    WHERE
      a.call_success
      AND b.call_success
  ),
  -- Users can sequently extend a staking NFT and add nxm as they go, the tops have to be summed over the lifetime of the nft to be able to track
  cumulative_extensions_on_single_nft AS (
    SELECT
      *,
      COUNT() OVER (
        PARTITION BY
          extention_call_tx_hash,
          nft_id,
          initial_tranche_id
      ) as cnt2,
      deposit_topup_amount / COUNT() OVER (
        PARTITION BY
          extention_call_tx_hash,
          nft_id,
          initial_tranche_id
      ) as summed_topup
    FROM
      deposits_with_extentions
  ),
  tranche_deposits AS (
    -- Add all deposits that have not been extended
    SELECT
      'non-extend-creation' AS reason,
      output_tokenId AS nft_id,
      trancheId AS tranche_id,
      -1 AS tranch_2,
      call_block_time AS ts,
      call_tx_hash,
      call_trace_address,
      CAST(amount AS DOUBLE) * 1E-18 AS staked_amount
    FROM
      non_extended_tranche_deposits
    UNION ALL
    -- Add all tranch expiries with start ts + tranch period of 91 days
    SELECT
      'non-extend-expiry' AS reason,
      output_tokenId AS nft_id,
      trancheId AS tranche_id,
      -1 AS tranch_2,
      from_unixtime(91.0 * 86400.0 * CAST(trancheId + 1 AS DOUBLE)) AS ts,
      call_tx_hash,
      call_trace_address,
      CAST(amount AS DOUBLE) * -1E-18 AS staked_amount
    FROM
      non_extended_tranche_deposits
    UNION ALL
    /*******************************************
    NFT & EXTENSIONS
     *******************************************/
    SELECT
      'original-nft-deposit' AS reason,
      output_tokenId AS nft_id,
      trancheId AS initial_tranche_id,
      0 AS extended_tranche_id,
      call_block_time AS ts,
      call_tx_hash,
      call_trace_address,
      amount * 1E-18 as net_deposit_change
    FROM
      original_extended_tranche_deposits
    UNION ALL -- initial deposit
    SELECT
      'original-nft-deposit-until-extension' AS reason,
      nft_id,
      initial_tranche_id,
      extended_tranche_id,
      extension_start_time AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit * -1 as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    UNION ALL
    SELECT
      'extended-addition' AS reason,
      nft_id,
      initial_tranche_id,
      extended_tranche_id,
      extension_start_time AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    UNION ALL
    SELECT
      'extended-addition-expiry' AS reason,
      nft_id,
      initial_tranche_id,
      extended_tranche_id,
      from_unixtime(
        91.0 * 86400.0 * CAST(extended_tranche_id + 1 AS DOUBLE)
      ) AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit * -1 as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    UNION ALL
    SELECT
      'nft-deposit-topups' AS reason,
      b.nft_id,
      b.initial_tranche_id,
      b.extended_tranche_id,
      a.extension_start_time as ts,
      a.deposit_call_tx_hash AS call_tx_hash,
      a.deposit_call_trace_address AS call_trace_address,
      b.summed_topup as net_deposit_change
    FROM
      cumulative_extensions_on_single_nft AS a
      INNER JOIN cumulative_extensions_on_single_nft AS b ON a.nft_id = b.nft_id
      AND a.initial_tranche_id = b.extended_tranche_id
    UNION ALL
    SELECT
      'nft-deposit-topups-expiry' AS reason,
      b.nft_id,
      b.initial_tranche_id,
      b.extended_tranche_id,
      from_unixtime(
        91.0 * 86400.0 * CAST(a.initial_tranche_id + 1 AS DOUBLE)
      ) AS ts,
      a.deposit_call_tx_hash AS call_tx_hash,
      a.deposit_call_trace_address AS call_trace_address,
      b.summed_topup * -1.0 as net_deposit_change
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
  v2_staking AS (
    SELECT DISTINCT
      date_trunc('day', ts) AS ts,
      SUM(staked_amount) OVER (
        PARTITION BY
          date_trunc('day', ts)
      ) AS nxm_staked
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
      DATE_TRUNC('day', ts) AS ts,
      pool_id,
      staked_amount
    FROM
      active_deposits AS a
      LEFT JOIN staker AS b ON a.call_tx_hash = b.call_tx_hash
      AND SLICE(
        b.call_trace_address,
        1,
        cardinality(a.call_trace_address)
      ) = a.call_trace_address
    UNION ALL
    SELECT
      DATE_TRUNC('day', a.call_block_time) AS ts,
      poolId AS pool_id,
      CAST(a.amount AS double) * -1E-18 AS burned_nxm
    FROM
      nexusmutual_ethereum.TokenController_call_burnStakedNXM AS a
      INNER JOIN nexusmutual_ethereum.StakingPool_call_burnStake AS b ON a.call_tx_hash = b.call_tx_hash
    WHERE
      b.contract_address != 0xcafeacf62fb96fa1243618c4727edf7e04d1d4ca
  ),
  v2_staked_per_pool_over_time AS (
    SELECT DISTINCT
      ts,
      pool_id,
      SUM(staked_amount) OVER (
        PARTITION BY
          pool_id
        ORDER BY
          ts
      ) AS total_nxm_staked
    FROM
      active_deposited_by_pool
  ),
  v2_staked_over_time AS (
    SELECT DISTINCT
      ts,
      SUM(staked_amount) OVER (
        PARTITION BY
          ts
      ) AS total_nxm_staked
    FROM
      active_deposited_by_pool
  ),
  v2_staked_per_pool_over_time_to_current_date AS (
    SELECT DISTINCT
      NOW() AS ts,
      pool_id,
      SUM(staked_amount) OVER (
        PARTITION BY
          pool_id
      ) AS total_nxm_staked
    FROM
      active_deposited_by_pool
    UNION
    SELECT
      ts,
      pool_id,
      total_nxm_staked
    FROM
      v2_staked_per_pool_over_time
  ),
  v2_staking_product_changes AS (
    SELECT
      start_time,
      finish_time,
      s.ts,
      DATE_TRUNC('day', ts) as t_trunc,
      COALESCE(t.pool_id, s.pool_id) as pool_id,
      product_id,
      CAST(total_nxm_staked AS DOUBLE) AS nxm_staked,
      weight,
      CAST(weight AS DOUBLE) * CAST(total_nxm_staked AS DOUBLE) / CAST(100 AS DOUBLE) AS total_stake_on_product
    FROM
      product_allocation_over_time as t
      INNER JOIN v2_staked_per_pool_over_time_to_current_date as s ON s.pool_id = t.pool_id
      AND s.ts <= finish_time
      AND start_time <= s.ts
  ),
  v2_total_staked AS (
    select DISTINCT
      t_trunc,
      pool_id,
      SUM(total_stake_on_product) OVER (
        PARTITION BY
          t_trunc,
          pool_id
      ) as total_at_time
    from
      v2_staking_product_changes
  ),
  pool_time_series AS (
    SELECT
      pool_id,
      ts
    FROM
      (
        SELECT DISTINCT
          pool_id
        FROM
          v2_staking_product_changes
      )
      CROSS JOIN (
        SELECT
          ts
        FROM
          UNNEST (
            sequence(CAST('2023-03-09' AS DATE), CAST(NOW() AS DATE))
          ) t (ts)
      )
  ),
  v2_product_allocated_to_fill AS (
    SELECT
      ts,
      a.pool_id as pool_id,
      total_at_time,
      COUNT(total_at_time) OVER (
        PARTITION BY
          a.pool_id
        ORDER BY
          ts
      ) AS pool_id_count
    FROM
      pool_time_series AS a
      LEFT JOIN v2_total_staked AS b ON a.ts = b.t_trunc
      AND a.pool_id = b.pool_id
  ),
  v2_staked_over AS (
    SELECT
      ts,
      pool_id,
      pool_id_count,
      total_at_time,
      COALESCE(
        FIRST_VALUE(total_at_time) OVER (
          PARTITION BY
            pool_id_count,
            pool_id
          ORDER BY
            ts
        ),
        0
      ) as total_allocated_per_pool
    FROM
      v2_product_allocated_to_fill
  ),
  v2_staked_over_time_all_pools AS (
    select distinct
      CAST(a.ts AS TIMESTAMP) as ts,
      SUM(total_allocated_per_pool) OVER (
        PARTITION BY
          a.ts
      ) as total_allocated
    FROM
      v2_staked_over AS a
  ),
  v1_allocated_over_time AS (
    SELECT DISTINCT
      ts,
      SUM(COALESCE(staked_amount, 0)) OVER (
        ORDER BY
          ts
      ) AS v1_net_allocated_amount
    FROM
      v1_staked
    WHERE
      ts < CAST('2023-03-09' AS DATE)
  ),
  staking_allocated_nxm_v1_v2 AS (
    SELECT
      COALESCE(a.day, b.ts) AS ts,
      COALESCE(amount, 0) AS v1_net_staked_nxm,
      COUNT(total_allocated) OVER (
        ORDER BY
          COALESCE(a.day, b.ts)
      ) AS total_allocated_count,
      total_allocated AS allocated_nxm_v2_staked
    FROM
      v1_nxm_staking_transactions AS a
      FULL JOIN v2_staked_over_time_all_pools AS b ON a.day = b.ts
  ),
  staking_allocated_nxm_v1_v2_filled AS (
    SELECT DISTINCT
      COALESCE(a.ts, c.ts) AS ts,
      v1_net_allocated_amount AS allocated_nxm_v1_staked,
      v1_net_staked_nxm,
      total_allocated_count,
      allocated_nxm_v2_staked,
      COALESCE(
        FIRST_VALUE(allocated_nxm_v2_staked) OVER (
          PARTITION BY
            total_allocated_count
          ORDER BY
            a.ts
        ),
        0
      ) as allocated_nxm_v2_staked_filled,
      COALESCE(
        SUM(b.total_nxm_staked) OVER (
          PARTITION BY
            a.ts
        ),
        0
      ) as v2_nxm_staked,
      COUNT(v1_net_allocated_amount) OVER (
        ORDER BY
          COALESCE(a.ts, c.ts) DESC
      ) AS v1_net_allocated_amount_count
    FROM
      staking_allocated_nxm_v1_v2 AS a
      LEFT JOIN v2_staked_over_time AS b ON a.ts = DATE_TRUNC('day', b.ts)
      FULL JOIN v1_allocated_over_time AS c ON a.ts = c.ts
  ),
  staking_over_time AS (
    SELECT DISTINCT
      ts,
      CASE
        WHEN ts < CAST('2023-03-09' AS DATE) THEN SUM(v1_net_staked_nxm) OVER (
          ORDER BY
            ts
        )
        ELSE 0
      END AS v1_net_staked_nxm_running,
      SUM(v2_nxm_staked) OVER (
        ORDER BY
          ts
      ) AS v2_nxm_staked,
      allocated_nxm_v1_staked,
      v1_net_allocated_amount_count,
      allocated_nxm_v2_staked_filled,
      CASE
        WHEN ts < CAST('2023-03-09' AS DATE) THEN SUM(v1_net_staked_nxm) OVER (
          ORDER BY
            ts
        )
        ELSE SUM(COALESCE(v2_nxm_staked, 0)) OVER (
          ORDER BY
            ts
        )
      END AS running_net_staked
    FROM
      staking_allocated_nxm_v1_v2_filled
  ),
  v1_v2_staking_all_filled AS (
    SELECT
      *,
      COALESCE(
        FIRST_VALUE(allocated_nxm_v1_staked) OVER (
          PARTITION BY
            v1_net_allocated_amount_count
          ORDER BY
            ts DESC
        ),
        0
      ) AS allocated_nxm_v1_staked_filled,
      COALESCE(allocated_nxm_v2_staked_filled, 0) + COALESCE(
        FIRST_VALUE(allocated_nxm_v1_staked) OVER (
          PARTITION BY
            v1_net_allocated_amount_count
          ORDER BY
            ts DESC
        ),
        0
      ) AS running_net_allocated
    FROM
      staking_over_time
    ORDER BY
      ts DESC
  )
SELECT
  *,
  running_net_allocated / running_net_staked AS leverage
FROM
  v1_v2_staking_all_filled
WHERE
  ts >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND ts <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  ts DESC