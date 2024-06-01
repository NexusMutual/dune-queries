WITH
  created_staking_pools AS (
    SELECT
      call_block_time,
      call_block_time as create_pool_ts,
      date_diff('day', call_block_time, NOW()) AS create_pool_days,
      output_0 AS pool_id,
      output_1 AS pool_address,
      maxPoolFee as max_pool_fee,
      isPrivatePool,
      json,
      initialPoolFee as initial_pool_fee,
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
    /*  SELECT DISTINCT
    s.call_block_time AS start_time,
    t.call_block_time AS finish_time,
    s.pool_id,
    s.product_id,
    s.target_weight AS weight
    FROM
    ranked_product_allocations AS s
    INNER JOIN ranked_product_allocations AS t ON t.product_id = s.product_id
    AND (t.pool_id = s.pool_id)
    AND (t.rank_entry = s.rank_entry + 1)
    UNION ALL*/
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
  pool_burns AS (
    SELECT DISTINCT
      poolId as pool_id,
      SUM(CAST(amount * -1E-18 AS DOUBLE)) OVER (
        PARTITION BY
          poolId
      ) as pool_nxm_burned
    FROM
      nexusmutual_ethereum.TokenController_call_burnStakedNXM as t
    WHERE
      call_success
  ),
  v2_staking AS (
    SELECT
      call_block_time as ts,
      poolId as pool_id,
      CAST(amount * 1E-18 AS DOUBLE) as nxm_staked
    FROM
      nexusmutual_ethereum.TokenController_call_depositStakedNXM
    WHERE
      call_success
    UNION ALL
    SELECT
      call_block_time as ts,
      poolId as pool_id,
      CAST(stakeToWithdraw * -1E-18 AS DOUBLE) as nxm_staked
    FROM
      nexusmutual_ethereum.TokenController_call_withdrawNXMStakeAndRewards as t
    WHERE
      t.stakeToWithdraw > CAST(0 AS UINT256)
      AND call_success
    UNION ALL
    SELECT
      call_block_time as ts,
      poolId as pool_id,
      CAST(amount * -1E-18 AS DOUBLE) as nxm_burned
    FROM
      nexusmutual_ethereum.TokenController_call_burnStakedNXM as t
    WHERE
      call_success
  ),
  v2_staked_per_pool_per_day AS (
    SELECT DISTINCT
      date_trunc('day', ts) as ts,
      pool_id,
      SUM(nxm_staked) OVER (
        PARTITION BY
          pool_id,
          date_trunc('day', ts)
      ) AS total_nxm_staked
    FROM
      v2_staking
  ),
  v2_staked_per_pool AS (
    SELECT DISTINCT
      pool_id,
      SUM(nxm_staked) OVER (
        PARTITION BY
          pool_id
      ) AS total_nxm_staked
    FROM
      v2_staking
  ),
  v2_staking_product_changes AS (
    SELECT
      COALESCE(t.pool_id, s.pool_id) as pool_id,
      product_id,
      CAST(total_nxm_staked AS DOUBLE) AS nxm_staked,
      weight,
      CAST(weight AS DOUBLE) * CAST(total_nxm_staked AS DOUBLE) / CAST(100 AS DOUBLE) AS total_stake_on_product
    FROM
      product_allocation_over_time as t
      INNER JOIN v2_staked_per_pool as s ON s.pool_id = t.pool_id
  ),
  v2_total_staked AS (
    SELECT DISTINCT
      pool_id,
      SUM(nxm_staked) OVER (
        PARTITION BY
          pool_id
      ) as total_pool_stake
    FROM
      v2_staking
  ),
  v2_total_allocated AS (
    select DISTINCT
      pool_id,
      SUM(total_stake_on_product) OVER (
        PARTITION BY
          pool_id
      ) as total_allocated_at_time
    from
      v2_staking_product_changes
  ),
  rewards AS (
    SELECT
      date_trunc('day', a.call_block_time) as ts,
      amount * 1E-18 * 86400.0 / CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) as reward_amount_nxm_per_day,
      poolId as pool_id
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards as b ON b.call_tx_hash = a.call_tx_hash
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
      ) as ts,
      amount * -1E-18 * 86400.0 / CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) as reward_amount_nxm_per_day,
      poolId as pool_id
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards as b ON b.call_tx_hash = a.call_tx_hash
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
      call_block_time as create_pool_ts,
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
    SELECT
    DISTINCT
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
      COALESCE(nxm_staked, 0) AS nxm_staked,
      COALESCE(reward_amount_nxm_per_day, 0) AS reward_amount_nxm_per_day
    FROM
      pool_all_days AS a
      LEFT JOIN v2_staking AS b ON CAST(a.ts AS DATE) = CAST(b.ts AS DATE)
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
  day_7_apy AS (
    SELECT DISTINCT
      pool_id,
      AVG(apy) OVER (
        PARTITION BY
          pool_id
      ) AS rolling_7day_apy
    FROM
      rolling_apy
    WHERE
      ts > date_add('day', -7, NOW())
  )
SELECT
  a.pool_id,
  rolling_7day_apy,
  rolling_30day_apy,
  rolling_90day_apy
FROM
  day_90_apy AS a
  INNER JOIN day_30_apy AS b ON a.pool_id = b.pool_id
  INNER JOIN day_7_apy AS c ON a.pool_id = c.pool_id