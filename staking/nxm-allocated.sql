WITH
  product_data AS (
    SELECT DISTINCT
      call_block_time,
      productParams
    FROM
      nexusmutual_ethereum.Cover_call_setProducts
    WHERE
      call_success
      AND contract_address = 0xcafeac0ff5da0a2777d915531bfa6b29d282ee62
    ORDER BY
      1
  ),
 product_set_raw AS (
    SELECT
      call_block_time,
      JSON_QUERY(
        productParamsOpened,
        'lax $.productName' OMIT QUOTES
      ) AS product_name,
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.productType'
        ) AS UINT256
      ) AS product_type_id,
      array_position(t.productParams, productParamsOpened) AS array_order
    FROM
      product_data AS t
      CROSS JOIN UNNEST (t.productParams) AS t (productParamsOpened)
    WHERE
      CAST(
        JSON_QUERY(productParamsOpened, 'lax $.productId') AS UINT256
      ) > CAST(1000000000000 AS UINT256)
  ),
  product_set AS (
    SELECT
      *,
      RANK() OVER (
        ORDER BY
          call_block_time ASC,
          array_order ASC
      ) - 1 AS product_id
    FROM
      product_set_raw
  ),
  product_type_data AS (
    SELECT DISTINCT
      call_block_time,
      productTypeParams
    FROM
      nexusmutual_ethereum.Cover_call_setProductTypes
    WHERE
      call_success
    ORDER BY
      call_block_time
  ),
 raw_product_types AS (
    SELECT
      *,
      CAST(
        JSON_QUERY(
          productTypeOpened,
          'lax $.productTypeName' OMIT QUOTES
        ) AS VARCHAR
      ) AS product_type_name,
      CAST(
        JSON_QUERY(
          productTypeOpened,
          'lax $.productTypeId' OMIT QUOTES
        ) AS VARCHAR
      ) AS product_type_id_input,
      array_position(t.productTypeParams, productTypeOpened) AS array_order
    FROM
      product_type_data AS t
      CROSS JOIN UNNEST (t.productTypeParams) AS t (productTypeOpened)
  ),
  product_types AS (
    SELECT
      call_block_time,
      CAST(
        RANK() OVER (
          ORDER BY
            call_block_time ASC,
            array_order ASC
        ) - 1 AS UINT256
      ) AS product_type_id,
      product_type_name
    FROM
      raw_product_types
    WHERE
      length(product_type_name) > 0
      AND CAST(product_type_id_input AS UINT256) > CAST(1000000 AS UINT256)
  ),
  v2_products AS (
    SELECT
      product_id,
      product_name,
      a.product_type_id,
      product_type_name
    FROM
      product_set AS a
      LEFT JOIN product_types AS b ON a.product_type_id = b.product_type_id
    ORDER BY
      product_id
  ),
  /*
  v1_staked AS (
  SELECT DISTINCT
  contractAddress AS product_address,
  SUM(amount * 1E-18) OVER (
  PARTITION BY
  contractAddress
  ) AS staked_amount
  FROM
  nexusmutual_ethereum.PooledStaking_evt_Staked
  ),
  v1_unstaked AS (
  SELECT DISTINCT
  contractAddress AS product_address,
  SUM(amount * 1E-18) OVER (
  PARTITION BY
  contractAddress
  ) AS unstaked_amount
  FROM
  nexusmutual_ethereum.PooledStaking_evt_Unstaked
  ),
  v1_net_staked AS (
  SELECT DISTINCT
  COALESCE(b.product_address, a.product_address) AS product_address,
  COALESCE(staked_amount, 0) - COALESCE(unstaked_amount, 0) AS net_staked_amount
  FROM
  v1_staked AS a
  FULL JOIN v1_unstaked AS b ON a.product_address = b.product_address
  ),
  v1_product_staked AS (
  SELECT
  product_address,
  net_staked_amount,
  product_name,
  product_type,
  syndicate
  FROM
  v1_net_staked
  LEFT JOIN v1_product_info ON v1_product_info.product_contract_address = v1_net_staked.product_address
  ),
   */
  created_staking_pools AS (
    SELECT
      call_block_time,
      call_block_time AS create_pool_ts,
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
      call_success = TRUE
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
      call_success = TRUE
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
          call_block_time DESC
      ) AS rank_entry
    FROM
      v2_product_allocated
  ),
  product_allocation AS (
    SELECT DISTINCT
      pool_id,
      product_id,
      target_weight
    FROM
      ranked_product_allocations
    WHERE
      rank_entry = 1
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
  v2_burns AS (
    SELECT DISTINCT
      poolId AS pool_id,
      SUM(CAST(amount AS double) * 1E-18 * 0.5) OVER (
        PARTITION BY
          poolId
      ) AS burned_nxm
    FROM
      nexusmutual_ethereum.TokenController_call_burnStakedNXM
  ),
  v2_staked_per_pool AS (
    SELECT DISTINCT
      t.pool_id,
      SUM(CAST(staked_amount AS DOUBLE)) OVER (
        PARTITION BY
          t.pool_id
      ) - COALESCE(burned_nxm, 0) AS total_nxm_staked,
      SUM(CAST(staked_amount AS DOUBLE)) OVER (
        PARTITION BY
          t.pool_id
      ) AS nxm_staked,
      COALESCE(burned_nxm, 0) AS v2_bruns
    FROM
      active_deposited_by_pool AS t
      LEFT JOIN v2_burns AS s ON t.pool_id = s.pool_id
  ),
  raw_staking_product_changes AS (
    SELECT
      t.pool_id AS pool_id,
      t.product_id AS product_id,
      total_nxm_staked,
      target_weight,
      (target_weight * total_nxm_staked) / 100 AS total_stake_on_product
    FROM
      product_allocation AS t
      INNER JOIN v2_staked_per_pool AS s ON s.pool_id = t.pool_id
  ),
  v2_staking_products AS (
    SELECT
      pool_id,
      t.product_id AS product_id,
      product_name,
      product_type_name,
      total_stake_on_product,
      SUM(total_stake_on_product) OVER (
        partition by
          product_name
      ) AS total_stake_on_product_per_product_name,
      --  ratio_weight,
      target_weight
    FROM
      raw_staking_product_changes AS t
      INNER JOIN v2_products AS u ON CAST(t.product_id AS UINT256) = CAST(u.product_id AS UINT256)
  )
SELECT
  *,
  SUM(total_stake_on_product_per_product_name) OVER () AS total_allocated
FROM
  v2_staking_products