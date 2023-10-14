WITH
  v1_product_info as (
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
  product_set AS (
    SELECT
      call_block_time,
      ROW_NUMBER() OVER () - 1 AS product_id,
      JSON_QUERY(
        productParamsOpened,
        'lax $.productName' OMIT QUOTES
      ) AS product_name,
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.productType'
        ) AS UINT256
      ) AS product_type_id
    FROM
      product_data as t
      CROSS JOIN UNNEST (t.productParams) as t (productParamsOpened)
    WHERE
      CAST(
        JSON_QUERY(productParamsOpened, 'lax $.productId') AS UINT256
      ) > CAST(1000000000000 AS UINT256)
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
      ) AS product_type_name
    FROM
      product_type_data as t
      CROSS JOIN UNNEST (t.productTypeParams) as t (productTypeOpened)
  ),
  product_types AS (
    SELECT
      CAST(ROW_NUMBER() OVER () - 1 AS UINT256) AS product_type_id,
      product_type_name
    FROM
      raw_product_types
    WHERE
      length(product_type_name) > 0
  ),
  v2_products AS (
    SELECT
      product_id,
      product_name,
      a.product_type_id,
      product_type_name
    FROM
      product_set as a
      LEFT JOIN product_types as b ON a.product_type_id = b.product_type_id
    ORDER BY
      product_id
  ),
  v1_reward_count as (
    select DISTINCT
      date_trunc('day', evt_block_time) as ts,
      contractAddress as product_address,
      SUM(amount * 1E-18) OVER (
        PARTITION BY
          date_trunc('day', evt_block_time),
          contractAddress
      ) as net_reward_amount
    from
      nexusmutual_ethereum.PooledStaking_evt_RewardAdded
  ),
  v1_burn_count as (
    select DISTINCT
      date_trunc('day', evt_block_time) as ts,
      contractAddress as product_address,
      SUM(amount * -1E-18) OVER (
        PARTITION BY
          date_trunc('day', evt_block_time),
          contractAddress
      ) as sum_burnt
    from
      nexusmutual_ethereum.PooledStaking_evt_Burned
  ),
  v1_rewards_burns AS (
    SELECT DISTINCT
      COALESCE(v1_reward_count.ts, v1_burn_count.ts) as day,
      COALESCE(
        v1_reward_count.product_address,
        v1_burn_count.product_address
      ) as product_address,
      COALESCE(net_reward_amount, 0) as v1_net_reward_amount,
      COALESCE(sum_burnt, 0) as v1_sum_burnt
    FROM
      v1_burn_count
      FULL JOIN v1_reward_count ON v1_reward_count.product_address = v1_burn_count.product_address
      AND v1_reward_count.ts = v1_burn_count.ts
  ),
  v2_rewards AS (
    SELECT
      a.call_block_time as cover_start_time,
      date_add(
        'second',
        CAST(JSON_QUERY(a.params, 'lax $.period') AS INT),
        a.call_block_time
      ) as cover_end_time,
      CAST(JSON_QUERY(a.params, 'lax $.productId') AS INT) AS product_id,
      CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) AS period,
      output_coverId as cover_id,
      amount * 1E-18 as reward_amount_nxm,
      amount * 1E-18 * 86400 / CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) as reward_amount_nxm_per_day,
      poolId as pool_id,
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
      ) as active_sequence
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards as b ON b.call_tx_hash = a.call_tx_hash
      INNER JOIN v2_products as c ON c.product_id = CAST(JSON_QUERY(a.params, 'lax $.productId') AS INT)
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
      v1_rewards_burns.day as day,
      v1_net_reward_amount as net_reward_amount,
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
  ) as running_reward,
  /*SUM(sum_burnt) OVER (
  ORDER BY
  day
  ) as total_burns,
  SUM(net_reward_amount) OVER (
  ORDER BY
  day
  ) + SUM(sum_burnt) OVER (
  ORDER BY
  day
  ) as net_running_reward,
  SUM(sum_burnt) OVER (
  PARTITION BY
  product_name
  ) as total_product_name_burnt,*/
  SUM(net_reward_amount) OVER (
    PARTITION BY
      syndicate
  ) as total_syndicate_reward_amount,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      product_name
  ) as total_product_name_reward_amount,
  CASE
    WHEN day > NOW() - interval '3' month AND day <= NOW() THEN product_name
    ELSE NULL
  END as total_product_name_latest,
  SUM(
    CASE
      WHEN day > NOW() - interval '3' month AND day <= NOW() THEN net_reward_amount
      ELSE NULL
    END
  ) OVER (
    PARTITION BY
      product_name, day > NOW() - interval '3' month AND day <= NOW()
  ) as total_product_name_reward_amount_by_latest,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      product_type
  ) as total_product_type_reward_amount
from
  v1_v2_rewards
  --WHERE
  --  day >= CAST('{{Start Date}}' AS TIMESTAMP)
  --  AND day <= CAST('{{End Date}}' AS TIMESTAMP)
WHERE
  day < NOW()
ORDER BY
  day