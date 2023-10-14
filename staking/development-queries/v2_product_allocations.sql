WITH
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
  v2_staking AS (
    SELECT
      call_block_time as ts,
      poolId as pool_id,
      CAST(amount * 1E-18 AS DOUBLE) as nxm_staked
    FROM
      nexusmutual_ethereum.TokenController_call_depositStakedNXM
    UNION ALL
    SELECT
      call_block_time as ts,
      poolId as pool_id,
      CAST(stakeToWithdraw * -1E-18 AS DOUBLE) as nxm_staked
    FROM
      nexusmutual_ethereum.TokenController_call_withdrawNXMStakeAndRewards as t
    WHERE
      t.stakeToWithdraw > CAST(0 AS UINT256)
  ),
  v2_staked_per_pool_over_time AS (
    SELECT DISTINCT
      ts,
      pool_id,
      SUM(nxm_staked) OVER (
        PARTITION BY
          pool_id
        ORDER BY
          ts
      ) AS total_nxm_staked
    FROM
      v2_staking
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
      INNER JOIN v2_staked_per_pool_over_time as s ON s.pool_id = t.pool_id
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
      ) total_allocated_per_pool
    FROM
      v2_product_allocated_to_fill
  )
select distinct
  ts,
  SUM(total_allocated_per_pool) OVER (
    PARTITION BY
      ts
  ) as total_allocated
FROM
  v2_staked_over
  