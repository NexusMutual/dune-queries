WITH
  product_info as (
    select DISTINCT
      product_contract_address as product_address,
      syndicate as syndicate,
      product_name as product_name,
      product_type as product_type,
      output_0 as v2_product_id
    from
      nexusmutual_ethereum.product_information as t
      FULL JOIN nexusmutual_ethereum.ProductsV1_call_getNewProductId as s ON t.product_contract_address = s.legacyProductId
    WHERE
      output_0 IS NOT NULL
      and call_success
  ),
  v1_cover_details as (
    select
      evt_block_time as cover_start_time,
      cid as v1_cover_id,
      premium * 1E-18 as premium,
      premiumNXM * 1E-18 as premium_nxm,
      scAdd as productContract,
      CAST(sumAssured AS DOUBLE) as sum_assured,
      syndicate,
      product_name,
      product_type,
      CASE
        WHEN b.call_success THEN 'NXM'
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS premium_asset,
      case
        when curr = 0x45544800 then 'ETH'
        when curr = 0x44414900 then 'DAI'
      end as cover_asset,
      from_unixtime(CAST(expiry AS DOUBLE)) as cover_end_time,
      evt_tx_hash as call_tx_hash
    from
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent as t
      LEFT JOIN product_info ON product_info.product_address = t.scAdd
      FULL JOIN nexusmutual_ethereum.Quotation_call_makeCoverUsingNXMTokens as b ON t.evt_tx_hash = b.call_tx_hash
    where
      (
        b.call_success
        and b.call_tx_hash is not null
      )
      or b.call_tx_hash is null
  ),
  v1_migrated_cover AS (
    SELECT
      cover_start_time,
      cover_end_time,
      premium,
      premium_nxm,
      coverIdV2 as cover_id,
      sum_assured,
      syndicate,
      product_name,
      product_type,
      cover_asset,
      premium_asset,
      newOwner as owner,
      call_tx_hash
    FROM
      nexusmutual_ethereum.CoverMigrator_evt_CoverMigrated as t
      INNER JOIN v1_cover_details ON v1_cover_details.v1_cover_id = t.coverIdV1
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
      call_block_time
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
      product_data as t
      CROSS JOIN UNNEST (t.productParams) as t (productParamsOpened)
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
      product_type_data as t
      CROSS JOIN UNNEST (t.productTypeParams) as t (productTypeOpened)
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
      product_set as a
      LEFT JOIN product_types as b ON a.product_type_id = b.product_type_id
    ORDER BY
      product_id
  ),
  v2_cover_brought AS (
    SELECT DISTINCT
      t.call_block_time,
      CAST(t.call_block_time AS TIMESTAMP) AS cover_start_time,
      CAST(output_coverId as UINT256) as cover_id,
      CAST(coverAmount AS DOUBLE) / 100.0 AS partial_cover_amount_in_nxm,
      CAST(
        JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
      ) AS commission_ratio,
      CAST(JSON_QUERY(params, 'lax $.productId') AS UINT256) AS product_id,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN 'DAI'
        ELSE 'NA'
      END as cover_asset,
      output_tokenPrice * 1E-18 * CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 / 100.0 AS  sum_assured_in_nxm,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 1 THEN 'DAI'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 255 THEN 'NXM'
        ELSE 'NA'
      END AS premium_asset,
      (
        1.0 + (
          CAST(
            JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
          ) / 10000.0
        )
      ) * CAST(output_premium AS DOUBLE) * 1E-18 AS premium_nxm,

      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 1 THEN 'DAI'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 255 THEN 'NXM'
        ELSE 'NA'
      END AS premium_asset,
      CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT) as expiry,
      date_add(
        'second',
        CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT),
        CAST(t.call_block_time AS TIMESTAMP)
      ) AS cover_end_time,
      CAST(
        JSON_QUERY(params, 'lax $.commissionDestination') AS VARBINARY
      ) AS commission_destination,
      CAST(
        JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
      ) / 10000.0 AS commission_ratio,
      1.0 + (
        CAST(
          JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
        ) / 10000.0
      ) AS cover_fee_multiplier,
      from_hex(
        trim(
          '"'
          FROM
            CAST(JSON_QUERY(params, 'lax $.owner') AS VARCHAR)
        )
      ) AS owner,
      t.call_tx_hash
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as t
      LEFT JOIN v2_products as s ON CAST(s.product_id AS UINT256) = CAST(
        JSON_QUERY(t.params, 'lax $.productId') AS UINT256
      )
      INNER JOIN nexusmutual_ethereum.StakingProducts_call_getPremium AS u ON t.call_tx_hash = u.call_tx_hash
      INNER JOIN nexusmutual_ethereum.Pool_call_getTokenPriceInAsset AS y ON t.call_tx_hash = y.call_tx_hash
    WHERE
      t.call_success
      AND u.call_success
      AND y.call_success
      AND u.contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4
      AND (
        t.call_trace_address IS NULL
        OR SLICE(
          u.call_trace_address,
          1,
          cardinality(t.call_trace_address)
        ) = t.call_trace_address
      )
  )
SELECT
  *
FROM
  v2_cover_brought