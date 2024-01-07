WITH
  v1_product_info AS (
    SELECT
      product_contract_address,
      product_name,
      product_type,
      syndicate
    FROM
      nexusmutual_ethereum.product_information
  ),
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
  v1_reward_count AS (
    select DISTINCT
      date_trunc('day', evt_block_time) AS ts,
      contractAddress AS product_address,
      SUM(amount * 1E-18) OVER (
        PARTITION BY
          date_trunc('day', evt_block_time),
          contractAddress
      ) AS net_reward_amount
    FROM
      nexusmutual_ethereum.PooledStaking_evt_RewardAdded
  ),
  v1_burn_count AS (
    select DISTINCT
      date_trunc('day', evt_block_time) AS ts,
      contractAddress AS product_address,
      SUM(amount * -1E-18) OVER (
        PARTITION BY
          date_trunc('day', evt_block_time),
          contractAddress
      ) AS sum_burnt
    FROM
      nexusmutual_ethereum.PooledStaking_evt_Burned
  ),
  v1_rewards_burns AS (
    SELECT DISTINCT
      COALESCE(v1_reward_count.ts, v1_burn_count.ts) AS day,
      COALESCE(
        v1_reward_count.product_address,
        v1_burn_count.product_address
      ) AS product_address,
      COALESCE(net_reward_amount, 0) AS v1_net_reward_amount,
      COALESCE(sum_burnt, 0) AS v1_sum_burnt
    FROM
      v1_burn_count
      FULL JOIN v1_reward_count ON v1_reward_count.product_address = v1_burn_count.product_address
      AND v1_reward_count.ts = v1_burn_count.ts
  ),
  v2_rewards AS (
    SELECT
      a.call_block_time AS cover_start_time,
      date_add(
        'second',
        CAST(JSON_QUERY(a.params, 'lax $.period') AS INT),
        a.call_block_time
      ) AS cover_end_time,
      CAST(JSON_QUERY(a.params, 'lax $.productId') AS INT) AS product_id,
      CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) AS period,
      output_coverId AS cover_id,
      amount * 1E-18 AS reward_amount_nxm,
      amount * 1E-18 * 86400 / CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) AS reward_amount_nxm_per_day,
      poolId AS pool_id,
      product_name,
      product_type_name,
      sequence(
        CAST(date_trunc('day', a.call_block_time) AS DATE),
        CAST(
          date_add(
            'second',
            CAST(JSON_QUERY(a.params, 'lax $.period') AS INT),
            a.call_block_time
          ) AS DATE
        )
      ) AS active_sequence
    FROM
      nexusmutual_ethereum.Cover_call_buyCover AS a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards AS b ON b.call_tx_hash = a.call_tx_hash
      INNER JOIN v2_products AS c ON c.product_id = CAST(JSON_QUERY(a.params, 'lax $.productId') AS INT)
    WHERE
      a.call_trace_address IS NULL
      OR SLICE(
        b.call_trace_address,
        1,
        cardinality(a.call_trace_address)
      ) = a.call_trace_address
  ),
  v1_v2_rewards AS (
    SELECT
      v1_rewards_burns.day AS day,
      v1_net_reward_amount AS net_reward_amount,
      v1_sum_burnt,
      syndicate,
      product_name,
      product_type
    FROM
      v1_rewards_burns
      LEFT JOIN v1_product_info ON v1_product_info.product_contract_address = v1_rewards_burns.product_address
    UNION
    SELECT
      day,
      reward_amount_nxm_per_day,
      CAST(0 AS DOUBLE) AS v1_amount_burnt,
      CAST(pool_id AS varchar),
      product_name,
      product_type_name
    FROM
      v2_rewards AS t
      CROSS JOIN UNNEST (t.active_sequence) AS t (day)
  )
select
  *,
  SUM(net_reward_amount) OVER (
    ORDER BY
      day
  ) AS running_reward,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      syndicate
  ) AS total_syndicate_reward_amount,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      product_name
  ) AS total_product_name_reward_amount,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      product_type
  ) AS total_product_type_reward_amount
FROM
  v1_v2_rewards
WHERE
  day < NOW()
ORDER BY
  day DESC