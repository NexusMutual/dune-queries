WITH
  product_info AS (
    select DISTINCT
      syndicate,
      product_name,
      product_type,
      output_0 AS v2_product_id,
      s.legacyProductId,
      t.product_contract_address AS product_contract_address
    from
      nexusmutual_ethereum.product_information AS t
      FULL JOIN nexusmutual_ethereum.ProductsV1_call_getNewProductId AS s ON CAST(t.product_contract_address AS VARCHAR) = CAST(s.legacyProductId AS VARCHAR)
  ),
  v1_burns_burned_burn_request AS (
    select
      evt_block_time,
      amount,
      contractAddress
    from
      nexusmutual_ethereum.PooledStaking_evt_Burned
  ),
  v1_burns AS (
    select
      evt_block_time AS day,
      CAST(amount AS DOUBLE) * 1E-18 AS sum_burnt,
      CAST('v1' AS varchar) AS syndicate,
      product_name,
      product_type,
      t.contractAddress AS contract_address_l
    from
      v1_burns_burned_burn_request AS t
      INNER JOIN product_info AS s ON CAST(s.product_contract_address AS VARCHAR) = CAST(t.contractAddress AS VARCHAR)
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
  v2_burns AS (
    SELECT
      call_block_time AS day,
      CAST(JSON_QUERY(params, 'lax $.productId') AS INT) AS product_id,
      amount * 1E-18 AS sum_burnt,
      '-' AS syndicate,
      product_name,
      product_type_name
    FROM
      nexusmutual_ethereum.StakingPool_call_burnStake AS t
      LEFT JOIN v2_products AS c ON c.product_id = CAST(JSON_QUERY(t.params, 'lax $.productId') AS INT)
    where
      call_success
  ),
  burns AS (
    SELECT
      day,
      sum_burnt,
      syndicate,
      product_name,
      product_type
    FROM
      v1_burns
    UNION ALL
    SELECT
      day,
      sum_burnt,
      syndicate,
      product_name,
      product_type_name AS product_type
    FROM
      v2_burns
  )
SELECT DISTINCT
  day,
  sum_burnt,
  SUM(sum_burnt) OVER (
    PARTITION BY
      product_name
  ) AS total_burnt_per_product,
  SUM(sum_burnt) OVER (
    ORDER BY
      day
  ) AS total_burnt,
  syndicate,
  product_name,
  product_type
FROM
  burns
WHERE
  day >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND day <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  day DESC NULLS FIRST