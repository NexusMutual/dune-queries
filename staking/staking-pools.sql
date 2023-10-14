WITH
  created_staking_pools AS (
    SELECT
      call_block_time,
      call_block_time AS create_pool_ts,
      date_diff('day', call_block_time, NOW()) AS create_pool_days,
      output_0 AS pool_id,
      output_1 AS pool_address,
      maxPoolFee AS max_pool_fee,
      isPrivatePool,
      json,
      initialPoolFee AS initial_pool_fee,
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
      weight AS target_weight,
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
      call_block_time AS start_time,
      NOW() AS finish_time,
      pool_id,
      product_id,
      target_weight AS weight
    FROM
      ranked_product_allocations
    WHERE
      rank_entry = count_entry
  ),
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
      a.call_block_time AS extension_start_time,
      amount * 1e-18 AS original_deposit,
      topUpAmount * 1e-18 AS deposit_topup_amount,
      initialTrancheId AS initial_tranche_id,
      newTrancheId AS extended_tranche_id,
      tokenId AS nft_id,
      a.call_tx_hash AS extention_call_tx_hash,
      b.call_block_time AS deposit_start_time,
      b.call_tx_hash AS deposit_call_tx_hash,
      b.call_trace_address AS deposit_call_trace_address
    FROM
      nexusmutual_ethereum.StakingPool_call_extendDeposit AS a
      LEFT JOIN nexusmutual_ethereum.StakingPool_call_depositTo AS b on --ON a.initialTrancheId = b.trancheId
      a.tokenId = b.output_tokenId
      AND b.contract_address != 0xcafeacf62fb96fa1243618c4727edf7e04d1d4ca
  ),
  -- Users can sequently extend a staking NFT and add nxm AS they go, the tops have to be summed over the lifetime of the nft to be able to track
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
      original_deposit + summed_topup AS net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    UNION ALL
    SELECT
      from_unixtime(
        91.0 * 86400.0 * CAST(extended_tranche_id + 1 AS DOUBLE)
      ) AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      (original_deposit + summed_topup) * -1 AS net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    UNION ALL
    SELECT
      deposit_start_time AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit * 1 AS net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    WHERE
      start_time_rank = 1
    UNION ALL -- initial deposit
    SELECT
      extension_start_time AS ts,
      deposit_call_tx_hash AS call_tx_hash,
      deposit_call_trace_address AS call_trace_address,
      original_deposit * -1 AS net_deposit_change
    FROM
      cumulative_extensions_on_single_nft
    WHERE
      start_time_rank = 1
    UNION ALL
    SELECT
      a.extension_start_time AS ts,
      a.deposit_call_tx_hash AS call_tx_hash,
      a.deposit_call_trace_address AS call_trace_address,
      (b.summed_topup + b.original_deposit) * -1.0 AS net_deposit_change
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
      (b.summed_topup + b.original_deposit) * 1.0 AS net_deposit_change
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
      a.poolId AS pool_id
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
  v2_staked_per_pool_per_day AS (
    SELECT DISTINCT
      date_trunc('day', ts) AS ts,
      pool_id,
      SUM(CAST(staked_amount AS DOUBLE)) OVER (
        PARTITION BY
          pool_id,
          date_trunc('day', ts)
      ) AS net_nxm_staked
    FROM
      active_deposited_by_pool
  ),
  burns AS (
    SELECT
      DATE_TRUNC('day', a.call_block_time) AS ts,
      CAST(a.amount AS double) * 1E-18 AS burned_nxm,
      poolId AS pool_id,
      CAST(JSON_QUERY(b.params, 'lax $.productId') AS INT) AS product_id
    FROM
      nexusmutual_ethereum.TokenController_call_burnStakedNXM AS a
      INNER JOIN nexusmutual_ethereum.StakingPool_call_burnStake AS b ON a.call_tx_hash = b.call_tx_hash
    WHERE
      b.contract_address != 0xcafeacf62fb96fa1243618c4727edf7e04d1d4ca
  ),
  burns_per_pool AS (
    SELECT DISTINCT
        pool_id,
        SUM(burned_nxm) OVER (PARTITION BY pool_id) AS pool_burned_nxm
    FROM
        burns
  ),
  gross_v2_staked_per_pool AS (
    SELECT DISTINCT
      pool_id,
      COALESCE( SUM(CAST(staked_amount AS DOUBLE)) OVER (
        PARTITION BY
          pool_id
      ), 0.0) AS total_nxm_staked
    FROM
      active_deposited_by_pool
  ),
  -- Take total burns per pool away from total current staked nxm
  net_v2_staked_per_pool AS 
  (
      SELECT
      a.pool_id,
      total_nxm_staked - COALESCE(CAST(pool_burned_nxm AS DOUBLE), 0.0) AS total_nxm_staked
    FROM
     gross_v2_staked_per_pool AS a
     LEFT JOIN burns_per_pool AS b ON a.pool_id = b.pool_id
  ),
  v2_staking_product_changes AS (
    SELECT
      COALESCE(t.pool_id, s.pool_id) AS pool_id,
      product_id,
      CAST(total_nxm_staked AS DOUBLE) AS nxm_staked,
      weight,
      CAST(weight AS DOUBLE) * CAST(total_nxm_staked AS DOUBLE) / CAST(100 AS DOUBLE) AS total_stake_on_product
    FROM
      product_allocation_over_time AS t
      INNER JOIN net_v2_staked_per_pool AS s ON s.pool_id = t.pool_id
  ),
  v2_total_allocated AS (
    select DISTINCT
      pool_id,
      SUM(total_stake_on_product) OVER (
        PARTITION BY
          pool_id
      ) AS total_allocated_at_time
    from
      v2_staking_product_changes
  ),
  rewards AS (
    SELECT
      date_trunc('day', a.call_block_time) AS ts,
      amount * 1E-18 * 86400.0 / CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) AS reward_amount_nxm_per_day,
      poolId AS pool_id
    FROM
      nexusmutual_ethereum.Cover_call_buyCover AS a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards AS b ON b.call_tx_hash = a.call_tx_hash
    WHERE
      a.call_trace_address IS NULL
      OR SLICE(
        b.call_trace_address,
        1,
        cardinality(a.call_trace_address)
      ) = a.call_trace_address
    UNION ALL
    SELECT
      date_trunc(
        'day',
        date_add(
          'second',
          CAST(JSON_QUERY(a.params, 'lax $.period') AS INT),
          a.call_block_time
        )
      ) AS ts,
      amount * -1E-18 * 86400.0 / CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) AS reward_amount_nxm_per_day,
      poolId AS pool_id
    FROM
      nexusmutual_ethereum.Cover_call_buyCover AS a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards AS b ON b.call_tx_hash = a.call_tx_hash
    WHERE
      a.call_trace_address IS NULL
      OR SLICE(
        b.call_trace_address,
        1,
        cardinality(a.call_trace_address)
      ) = a.call_trace_address
  ),
  pool_created AS (
    SELECT
      call_block_time AS create_pool_ts,
      output_0 AS pool_id,
      sequence(
        CAST(date_trunc('day', call_block_time) AS DATE),
        CAST(NOW() AS DATE)
      ) AS active_sequence
    from
      nexusmutual_ethereum.Cover_call_createStakingPool
    WHERE
      call_success
      AND contract_address = 0xcafeac0fF5dA0A2777d915531bfA6B29d282Ee62
  ),
  pool_all_days AS (
    SELECT
      ts,
      pool_id
    FROM
      pool_created AS t
      CROSS JOIN UNNEST (t.active_sequence) AS t (ts)
  ),
  rewards_per_day_per_pool AS (
    SELECT DISTINCT
      ts,
      pool_id,
      SUM(reward_amount_nxm_per_day) OVER (
        PARTITION BY
          ts,
          pool_id
      ) AS reward_amount_nxm_per_day
    FROM
      rewards
  ),
  rewards_stake_since_creation AS (
    SELECT
      a.ts AS ts,
      a.pool_id AS pool_id,
      COALESCE(net_nxm_staked, 0) AS nxm_staked,
      COALESCE(reward_amount_nxm_per_day, 0) AS reward_amount_nxm_per_day
    FROM
      pool_all_days AS a
      LEFT JOIN v2_staked_per_pool_per_day AS b ON CAST(a.ts AS DATE) = CAST(b.ts AS DATE)
      AND CAST(a.pool_id AS UINT256) = CAST(b.pool_id AS UINT256)
      LEFT JOIN rewards_per_day_per_pool AS c ON CAST(a.ts AS DATE) = CAST(c.ts AS DATE)
      AND CAST(a.pool_id AS UINT256) = CAST(c.pool_id AS UINT256)
  ),
  rolling_rewards_staking AS (
    SELECT DISTINCT
      ts,
      pool_id,
      SUM(nxm_staked) OVER (
        PARTITION BY
          pool_id
        ORDER BY
          ts
      ) AS rolling_nxm_staked,
      SUM(reward_amount_nxm_per_day) OVER (
        PARTITION BY
          pool_id
        ORDER BY
          ts
      ) AS rolling_reward_per_day
    FROM
      rewards_stake_since_creation
    ORDER BY
      pool_id,
      ts
  ),
  rolling_apy AS (
    SELECT
      ts,
      pool_id,
      CASE
        WHEN rolling_nxm_staked != 0 THEN CAST(rolling_reward_per_day AS DOUBLE) / CAST(rolling_nxm_staked AS DOUBLE)
        ELSE 0
      END AS apd,
      CASE
        WHEN rolling_nxm_staked != 0 THEN CAST(rolling_reward_per_day AS DOUBLE) * 36500.0 / CAST(rolling_nxm_staked AS DOUBLE)
        ELSE 0
      END AS apy
    FROM
      rolling_rewards_staking
  ),
  day_30_apy AS (
    SELECT DISTINCT
      pool_id,
      AVG(apy) OVER (
        PARTITION BY
          pool_id
      ) AS rolling_30day_apy
    FROM
      rolling_apy
    WHERE
      ts > date_add('day', -30, NOW())
  ),
  day_90_apy AS (
    SELECT DISTINCT
      pool_id,
      AVG(apy) OVER (
        PARTITION BY
          pool_id
      ) AS rolling_90day_apy
    FROM
      rolling_apy
    WHERE
      ts > date_add('day', -90, NOW())
  ),
  pool_ipfs AS (
    SELECT DISTINCT
      ipfsDescriptionHash,
      output_0 AS pool_id
    from
      nexusmutual_ethereum.Cover_call_createStakingPool
    WHERE
      call_success
  ),
  pool_fee_changes_over_time AS (
    SELECT DISTINCT
      call_block_time,
      output_0 AS pool_id,
      initialPoolFee AS pool_fee
    from
      nexusmutual_ethereum.Cover_call_createStakingPool
    WHERE
      call_success
    UNION
    SELECT
      call_block_time,
      pool_id,
      newFee AS pool_fee
    FROM
      nexusmutual_ethereum.StakingPool_call_setPoolDescription AS a
      INNER JOIN pool_ipfs AS c ON a.ipfsDescriptionHash = c.ipfsDescriptionHash
      INNER JOIN (
        SELECT
          call_tx_hash,
          newFee
        FROM
          nexusmutual_ethereum.StakingPool_call_setPoolFee
      ) AS b ON a.call_tx_hash = b.call_tx_hash
  ),
  fees_ranked_and_counted AS (
    SELECT
      call_block_time,
      pool_id,
      pool_fee,
      RANK() OVER (
        PARTITION BY
          pool_id
        ORDER BY
          call_block_time ASC
      ) AS rank_of_fee,
      COUNT() OVER (
        PARTITION BY
          pool_id
      ) AS count_of_fee
    FROM
      pool_fee_changes_over_time
  ),
  pool_fees_over_time AS (
    SELECT
      a.call_block_time AS start_time,
      b.call_block_time AS finish_time,
      a.pool_id,
      a.pool_fee AS pool_fee
    FROM
      fees_ranked_and_counted AS a
      INNER JOIN fees_ranked_and_counted AS b ON a.pool_id = b.pool_id
      AND ((a.rank_of_fee + 1 = b.rank_of_fee))
    UNION
    SELECT
      call_block_time AS start_time,
      NOW() AS finish_time,
      pool_id,
      pool_fee
    FROM
      fees_ranked_and_counted
    WHERE
      rank_of_fee = count_of_fee
  ),
  get_premiums AS (
    SELECT
      a.call_block_time AS ts,
      a.call_tx_hash AS call_tx_hash,
      c.start_time AS fee_start_time,
      c.finish_time AS finish_start_time,
      c.pool_id AS fee_pool_id,
      poolId AS pool_id,
      productId AS product_id,
      period,
      output_coverId AS cover_id,
      CAST(
        JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
      ) / 100 AS commission_ratio,
      JSON_QUERY(params, 'lax $.commissionDestination') AS commission_address,
      CAST(output_premium AS DOUBLE) * 1E-18 AS premium,
      CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 AS commission,
      CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * CAST(pool_fee AS DOUBLE) / 100.0 AS pool_manager_commission,
      CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * (1 - (CAST(pool_fee AS DOUBLE) / 100.0)) AS staker_commission,
      CASE
        WHEN date_add(
          'second',
          CAST(period AS INT),
          CAST(a.call_block_time AS TIMESTAMP)
        ) > NOW() THEN CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * (
          to_unixTime(NOW()) - to_unixTime(CAST(a.call_block_time AS TIMESTAMP))
        ) / CAST(period AS DOUBLE)
        ELSE CAST(output_premium AS DOUBLE) * 1E-18 * 0.5
      END AS commission_emitted,
      (
        CAST(
          JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
        ) / 10000.0
      ) * CAST(output_premium AS DOUBLE) * 1E-18 AS commission_distibutor_fee,
      CASE
        WHEN date_add(
          'second',
          CAST(period AS INT),
          CAST(a.call_block_time AS TIMESTAMP)
        ) > NOW() THEN CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * CAST(pool_fee AS DOUBLE) * (
          to_unixTime(NOW()) - to_unixTime(CAST(a.call_block_time AS TIMESTAMP))
        ) / (CAST(period AS DOUBLE) * 100.0)
        ELSE CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * CAST(pool_fee AS DOUBLE) / 100.0
      END AS pool_manager_commission_emitted,
      CASE
        WHEN date_add(
          'second',
          CAST(period AS INT),
          CAST(a.call_block_time AS TIMESTAMP)
        ) > NOW() THEN CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * CAST(pool_fee AS DOUBLE) * (
          to_unixTime(NOW()) - to_unixTime(CAST(a.call_block_time AS TIMESTAMP))
        ) / (CAST(period AS DOUBLE) * 100.0)
        ELSE CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * (1 - CAST(pool_fee AS DOUBLE) / 100.0)
      END AS staker_commission_emitted
    FROM
      nexusmutual_ethereum.StakingProducts_call_getPremium AS a
      INNER JOIN pool_fees_over_time AS c ON a.poolId = c.pool_id
      AND c.start_time <= a.call_block_time
      AND a.call_block_time <= c.finish_time
      INNER JOIN nexusmutual_ethereum.Cover_call_buyCover AS b ON a.call_tx_hash = b.call_tx_hash
      AND a.productId = CAST(
        JSON_QUERY(b.params, 'lax $.productId') AS UINT256
      )
      AND a.poolId = CAST(
        JSON_QUERY(b.poolAllocationRequests[1], 'lax $.poolId') AS UINT256
      )
    WHERE
      a.call_success
      AND b.call_success
      AND a.contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4
  ),
  commission AS (
    SELECT DISTINCT
      pool_id,
      SUM(commission) OVER (
        PARTITION BY
          pool_id
      ) AS total_commission,
      SUM(commission_emitted) OVER (
        PARTITION BY
          pool_id
      ) AS total_commission_emitted,
      SUM(pool_manager_commission_emitted) OVER (
        PARTITION BY
          pool_id
      ) AS pool_manager_commission_emitted,
      SUM(pool_manager_commission) OVER (
        PARTITION BY
          pool_id
      ) AS pool_manager_commission,
      SUM(staker_commission_emitted) OVER (
        PARTITION BY
          pool_id
      ) AS staker_commission_emitted,
      SUM(staker_commission) OVER (
        PARTITION BY
          pool_id
      ) AS staker_commission,
      SUM(commission_distibutor_fee) OVER (
        PARTITION BY
          pool_id
      ) AS pool_distributor_commission
    FROM
      get_premiums
  ),
  request_allocation AS (
    SELECT
      *
    FROM
      nexusmutual_ethereum.StakingPool_call_requestAllocation
    WHERE
      call_success = TRUE
  ),
  product_allocations AS (
    SELECT
      s.call_block_time AS date_time,
      period,
      t.pool_id AS pool_id,
      t.product_id AS product_id,
      amount * 1E-18 AS cover_amount,
      RANK() OVER (
        PARTITION BY
          t.pool_id,
          t.product_id
        ORDER BY
          s.call_block_time DESC
      ) AS pool_product_update_rank
    FROM
      request_allocation AS s
      INNER JOIN get_premiums AS t ON t.call_tx_hash = s.call_tx_hash
  ),
  -- 
  pool_fee_changes AS (
    SELECT
      ipfsDescriptionHash,
      newFee,
      RANK() OVER (
        PARTITION BY
          ipfsDescriptionHash
        ORDER BY
          call_block_time DESC
      ) AS ranked
    FROM
      nexusmutual_ethereum.StakingPool_call_setPoolDescription AS a
      INNER JOIN (
        SELECT
          call_tx_hash,
          newFee
        FROM
          nexusmutual_ethereum.StakingPool_call_setPoolFee
      ) AS b ON a.call_tx_hash = b.call_tx_hash
  ),
  latest_pool_fee_changes AS (
    select
      a.ipfsDescriptionHash AS ipfsDescriptionHash,
      pool_id,
      newFee
    from
      pool_ipfs AS a
      INNER JOIN pool_fee_changes AS b ON a.ipfsDescriptionHash = b.ipfsDescriptionHash
      and b.ranked = 1
  ),
  pool_manager_address AS (
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
  pool_managers AS (
    select
      *
    from
      pool_manager_address
    where
      ranked = 1
  ),
  pools AS (
    SELECT DISTINCT
      create_pool_ts,
      CAST(t.pool_id AS UINT256) AS pool_id,
      pool_address,
      manager_address,
      COALESCE(x.newFee, initial_pool_fee) AS current_pool_fee,
      max_pool_fee,
      isPrivatePool,
      COALESCE(CAST(total_nxm_staked AS DOUBLE), 0) AS net_nxm_staked,
      COALESCE(total_allocated_at_time, 0) AS total_allocate_by_pool,
      COALESCE(pool_distributor_commission, 0) AS pool_distributor_commission,
      COALESCE(pool_manager_commission, 0) AS pool_manager_commission,
      COALESCE(pool_manager_commission_emitted, 0) AS pool_manager_commission_emitted,
      COALESCE(staker_commission_emitted, 0) AS staker_commission_emitted,
      COALESCE(staker_commission, 0) AS staker_commission,
      total_nxm_staked,
      COALESCE( pool_burned_nxm, 0) AS pool_nxm_burned,
      rolling_90day_apy,
      rolling_30day_apy
    FROM
      pool_managers AS t
      LEFT JOIN day_30_apy AS s ON t.pool_id = s.pool_id
      LEFT JOIN day_90_apy AS v ON t.pool_id = v.pool_id
      LEFT JOIN net_v2_staked_per_pool AS m ON t.pool_id = m.pool_id
      LEFT JOIN v2_total_allocated AS u ON t.pool_id = u.pool_id
      LEFT JOIN latest_pool_fee_changes AS x ON x.pool_id = t.pool_id
      LEFT JOIN created_staking_pools AS y ON y.pool_id = t.pool_id
      LEFT JOIN commission AS z ON z.pool_id = t.pool_id
      LEFT JOIN burns_per_pool  AS b ON b.pool_id = t.pool_id
  )
SELECT
  create_pool_ts,
  CAST(pool_id AS UINT256) AS pool_id,
  rolling_90day_apy,
  rolling_30day_apy,
  CASE
    WHEN isPrivatePool THEN 'Private'
    ELSE 'Public'
  END AS isPrivatePool,
  net_nxm_staked AS net_nxm_staked,
  total_allocate_by_pool,
  CASE
    WHEN net_nxm_staked > 0 THEN total_allocate_by_pool / net_nxm_staked
    ELSE 0
  END AS leverage,
  current_pool_fee,
  max_pool_fee,
  pool_manager_commission + pool_distributor_commission + staker_commission AS total_commission,
  pool_distributor_commission,
  staker_commission_emitted,
  staker_commission - staker_commission_emitted AS future_staker_commission,
  pool_manager_commission_emitted,
  pool_manager_commission - pool_manager_commission_emitted AS future_pool_manager_commission,
  pool_nxm_burned,
  COALESCE(
    SUM(net_nxm_staked * rolling_30day_apy) OVER () / (SUM(net_nxm_staked) OVER ()),
    0
  ) AS weighted_average_apy,
  MAX(rolling_30day_apy) OVER () AS max_apy,
  pool_address,
  manager_address,
  SUM(net_nxm_staked) OVER () AS total_staked_nxm_over_all_pool_check,
  SUM(total_allocate_by_pool) OVER () AS total_allocated_nxm_over_all_pool_check
FROM
  pools
ORDER BY
  pool_id