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
      call_block_time ASC
  ),
  product_set AS (
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
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.coverAssets'
        ) AS UINT256
      ) AS cover_asset,
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.initialPriceRatio'
        ) AS UINT256
      ) AS initial_price_ratio,
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.capacityReductionRatio'
        ) AS UINT256
      ) AS capacity_reduction_ratio,
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.isDeprecated'
        ) AS BOOLEAN
      ) AS is_deprecated,
      CAST(
        JSON_QUERY(
          JSON_QUERY(productParamsOpened, 'lax $.product' OMIT QUOTES),
          'lax $.useFixedPrice'
        ) AS BOOLEAN
      ) AS used_fixed_price,
      CAST(
        JSON_QUERY(productParamsOpened, 'lax $.productId') AS UINT256
      ) as raw_product_id
    FROM
      product_data as t
      CROSS JOIN UNNEST (t.productParams) as t (productParamsOpened)
    WHERE
      CAST(
        JSON_QUERY(productParamsOpened, 'lax $.productId') AS UINT256
      ) > CAST(1000000000000 AS UINT256)
  ),
  product_name_processed AS (
    SELECT
      *,
      RANK() OVER (
        ORDER BY
          call_block_time ASC,
          lower(product_name) ASC
      ) - 1 AS product_id
    FROM
      product_set
  )
SELECT
  *
FROM
  product_name_processed