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
  rewards AS (
    SELECT
      a.call_block_time AS cover_start_time,
      date_add('second', CAST(period AS INT), a.call_block_time) AS cover_end_time,
      CAST(productId AS UINT256) AS product_id,
      period,
      output_coverId AS cover_id,
      CAST(output_premium AS DOUBLE) * 1E-18 AS premium,
      CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 AS commission,
      CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * CAST(pool_fee AS DOUBLE) / 100.0 AS pool_manager_commission,
      CAST(output_premium AS DOUBLE) * 1E-18 * 0.5 * (1 - (CAST(pool_fee AS DOUBLE) / 100.0)) AS staker_commission,
      (
        CAST(
          JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
        ) / 10000.0
      ) * CAST(output_premium AS DOUBLE) * 1E-18 AS commission_distibutor_fee,
      poolId AS pool_id
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
  rewards_in_three_months AS (
    SELECT
      pool_id,
      cover_start_time,
      cover_end_time,
      period,
      a.product_id,
      product_name,
      product_type_name,
      commission,
      pool_manager_commission AS pool_manager_commission_in_last_three_months,
      staker_commission AS staker_commission_in_last_three_months,
      commission_distibutor_fee AS commission_distibutor_fee_in_last_three_months,
      CASE
        WHEN date_add(
          'second',
          CAST(period AS BIGINT),
          cover_start_time
        ) > NOW() THEN pool_manager_commission * (
          to_unixTime(NOW()) - to_unixTime(cover_start_time)
        ) / CAST(period AS DOUBLE)
        ELSE pool_manager_commission
      END AS streamed_pool_manager_commission_in_last_three_months,
      CASE
        WHEN date_add(
          'second',
          CAST(period AS BIGINT),
          cover_start_time
        ) > NOW() THEN staker_commission * (
          to_unixTime(NOW()) - to_unixTime(cover_start_time)
        ) / CAST(period AS DOUBLE)
        ELSE staker_commission
      END AS streamed_staker_commission_in_last_three_months
    FROM
      rewards AS a
      INNER JOIN v2_products AS b ON a.product_id = CAST(b.product_id AS UINT256)
    WHERE
      date_add('month', 3, cover_start_time) > NOW()
  ),
  comission_on_product AS (
    SELECT DISTINCT
      CAST(product_id AS UINT256) AS product_id,
      product_name,
      product_type_name,
      SUM(pool_manager_commission_in_last_three_months) OVER (
        PARTITION BY
          product_name
      ) AS total_pool_manager_commission,
      SUM(staker_commission_in_last_three_months) OVER (
        PARTITION BY
          product_name
      ) AS total_staker_commission,
      SUM(commission_distibutor_fee_in_last_three_months) OVER (
        PARTITION BY
          product_name
      ) AS total_commission_distibutor_fee,
      SUM(
        streamed_pool_manager_commission_in_last_three_months
      ) OVER (
        PARTITION BY
          product_name
      ) AS streamed_pool_manager_commission,
      SUM(streamed_staker_commission_in_last_three_months) OVER (
        PARTITION BY
          product_name
      ) AS streamed_staker_commission
    FROM
      rewards_in_three_months
  )
SELECT
  product_id,
  product_name,
  product_type_name,
  total_commission_distibutor_fee + streamed_pool_manager_commission + streamed_staker_commission AS reward_streamed,
  total_staker_commission + total_pool_manager_commission - streamed_pool_manager_commission - streamed_staker_commission AS reward_remaining
FROM
  comission_on_product
ORDER BY
  product_name ASC