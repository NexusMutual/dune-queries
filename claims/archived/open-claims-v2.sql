WITH
  v2_claims AS (
    SELECT
      CAST(coverId AS UINT256) AS cover_id,
      ipfsMetadata,
      call_block_time AS claim_submit_time,
      CAST(
        JSON_QUERY(output_claim, 'lax $.assessmentId') AS UINT256
      ) AS assessment_id,
      CAST(
        JSON_QUERY(output_claim, 'lax $.amount') AS DOUBLE
      ) * 1E-18 AS amount_requested,
      CASE
        WHEN CAST(
          JSON_QUERY(output_claim, 'lax $.coverAsset') AS INT
        ) = 0 THEN 'ETH'
        WHEN CAST(
          JSON_QUERY(output_claim, 'lax $.coverAsset') AS INT
        ) = 1 THEN 'DAI'
        ELSE 'NA'
      END AS cover_asset,
      owner
    FROM
      nexusmutual_ethereum.IndividualClaims_call_submitClaimFor
    UNION
    SELECT
      CAST(coverId AS UINT256) AS cover_id,
      ipfsMetadata,
      call_block_time AS claim_submit_time,
      CAST(
        JSON_QUERY(output_claim, 'lax $.assessmentId') AS UINT256
      ) AS assessment_id,
      CAST(
        JSON_QUERY(output_claim, 'lax $.amount') AS DOUBLE
      ) * 1E-18 AS amount_request,
      CASE
        WHEN CAST(
          JSON_QUERY(output_claim, 'lax $.coverAsset') AS INT
        ) = 0 THEN 'ETH'
        WHEN CAST(
          JSON_QUERY(output_claim, 'lax $.coverAsset') AS INT
        ) = 1 THEN 'DAI'
        ELSE 'NA'
      END AS cover_asset,
      CAST(NULL AS varbinary) AS owner
    FROM
      nexusmutual_ethereum.IndividualClaims_call_submitClaim
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
      CAST(product_id AS UINT256) AS product_id,
      product_name,
      a.product_type_id,
      product_type_name
    FROM
      product_set AS a
      LEFT JOIN product_types AS b ON a.product_type_id = b.product_type_id
    ORDER BY
      product_id
  ),
  v1_product_ids AS (
    SELECT DISTINCT
      legacyProductId AS v1_product_address,
      output_0 AS v2_product_id
    FROM
      nexusmutual_ethereum.ProductsV1_call_getNewProductId
    WHERE call_success
  ),
  v1_v2_product_info AS (
    SELECT
      *
    FROM
      v2_products AS a
      LEFT JOIN v1_product_ids AS b ON a.product_id = b.v2_product_id
  ),
  v1_cover_details AS (
    SELECT
      cid AS v1_cover_id,
      evt_block_time AS cover_start_time,
      scAdd AS productContract,
      CAST(sumAssured AS DOUBLE) AS sum_assured,
      'v1' AS syndicate,
      product_name,
      product_type_name AS product_type,
      CASE
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS cover_asset,
      from_unixtime(CAST(expiry AS DOUBLE)) AS cover_end_time
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent AS t
      LEFT JOIN v1_v2_product_info AS s ON s.v1_product_address = t.scAdd
  ),
  v1_migrated_cover AS (
    SELECT
      cover_start_time,
      cover_end_time,
      coverIdV2 AS cover_id,
      sum_assured,
      syndicate,
      product_name,
      product_type,
      cover_asset,
      newOwner AS owner
    FROM
      nexusmutual_ethereum.CoverMigrator_evt_CoverMigrated AS t
      INNER JOIN v1_cover_details ON v1_cover_details.v1_cover_id = t.coverIdV1
  ),
  v2_cover_brought AS (
    SELECT
      CAST(call_block_time AS TIMESTAMP) AS cover_start_time,
      CAST(output_coverId AS UINT256) AS cover_id,
      CAST(JSON_QUERY(params, 'lax $.productId') AS UINT256) AS product_id,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.coverAsset') AS INT) = 1 THEN 'DAI'
        ELSE 'NA'
      END AS cover_asset,
      CAST(JSON_QUERY(params, 'lax $.amount') AS DOUBLE) * 1E-18 AS sum_assured,
      CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT) AS expiry,
      date_add(
        'second',
        CAST(JSON_QUERY(params, 'lax $.period') AS BIGINT),
        CAST(call_block_time AS TIMESTAMP)
      ) AS cover_end_time,
      CAST(
        JSON_QUERY(params, 'lax $.commissionDestination') AS VARBINARY
      ) AS commission_destination,
      JSON_QUERY(poolAllocationRequests[1], 'lax $.poolId') AS pool_id,
      product_name,
      product_type_name AS product_type,
      from_hex(
        trim(
          '"'
          FROM
            CAST(JSON_QUERY(params, 'lax $.owner') AS VARCHAR)
        )
      ) AS owner
    FROM
      nexusmutual_ethereum.Cover_call_buyCover AS t
      INNER JOIN v1_v2_product_info AS s ON s.product_id = CAST(
        JSON_QUERY(t.params, 'lax $.productId') AS UINT256
      )
  ),
  covers AS (
    SELECT
      cover_start_time,
      cover_end_time,
      cover_id,
      sum_assured,
      pool_id AS syndicate,
      product_name,
      product_type,
      cover_asset,
      owner
    FROM
      v2_cover_brought
    UNION
    SELECT
      cover_start_time,
      cover_end_time,
      cover_id,
      sum_assured,
      syndicate,
      product_name,
      product_type,
      cover_asset,
      owner
    FROM
      v1_migrated_cover
    ORDER BY
      cover_id
  ),
  d_prices AS (
    SELECT DISTINCT
      DATE_TRUNC('minute', minute) AS minute,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          symbol
        ORDER BY
          DATE_TRUNC('minute', minute)
      ) AS price_dollar
    FROM prices.usd
    WHERE minute > CAST('2019-05-01' AS TIMESTAMP)
      and ((symbol = 'ETH' and blockchain is null)
        or (symbol = 'DAI' and blockchain = 'ethereum'))
  ),
  covers_with_price AS (
    SELECT
      cover_start_time,
      cover_end_time,
      cover_id,
      sum_assured,
      syndicate,
      product_name,
      product_type,
      cover_asset,
      owner,
      price_dollar
    FROM
      covers AS t
      LEFT JOIN d_prices ON d_prices.minute = DATE_TRUNC('minute', t.cover_start_time)
      AND d_prices.symbol = t.cover_asset
  ),
  vote_count AS (
    SELECT DISTINCT
      assessmentId AS assessment_id,
      SUM(
        CASE
          WHEN accepted = true THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          assessmentId
      ) AS yes_votes,
      SUM(
        CASE
          WHEN accepted = false THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          assessmentId
      ) AS no_votes,
      SUM(
        CASE
          WHEN accepted = true THEN CAST(stakedAmount AS DOUBLE) * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          assessmentId
      ) AS yes_nxm_votes,
      SUM(
        CASE
          WHEN accepted = false THEN CAST(stakedAmount AS DOUBLE) * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          assessmentId
      ) AS no_nxm_votes
    FROM
      nexusmutual_ethereum.Assessment_evt_VoteCast
  ),
  v2_open_claims AS (
    SELECT
      v2_claims.assessment_id,
      v2_claims.cover_id,
      'PENDING ❓' AS verdict,
      CONCAT(
        '<a href="https://app.nexusmutual.io/assessment/claim/claim-details?claimId=',
        CAST(v2_claims.assessment_id AS VARCHAR),
        '" target="_blank">',
        'link',
        '</a>'
      ) AS url_link,
      product_name,
      product_type,
      syndicate AS staking_pool,
      covers_with_price.cover_asset,
      sum_assured AS cover_amount,
      covers_with_price.price_dollar * sum_assured AS dollar_cover_amount,
      amount_requested AS claim_amount,
      s.price_dollar * amount_requested AS dollar_claim_amount,
      cover_start_time,
      cover_end_time,
      claim_submit_time,
      yes_votes,
      no_votes,
      yes_nxm_votes,
      no_nxm_votes
    FROM
      v2_claims
      INNER JOIN vote_count ON vote_count.assessment_id = v2_claims.assessment_id
      INNER JOIN covers_with_price ON covers_with_price.cover_id = v2_claims.cover_id
      INNER JOIN d_prices AS s ON s.minute = DATE_TRUNC('minute', v2_claims.claim_submit_time)
      AND s.symbol = v2_claims.cover_asset
    WHERE
      v2_claims.assessment_id NOT IN (
        select distinct
          CAST(_0 AS UINT256)
        from
          nexusmutual_ethereum.Assessment_call_assessments
      )
  )
SELECT
  CAST(assessment_id AS BIGINT) AS assessment_id,
  cover_id,
  verdict,
  url_link,
  product_name,
  product_type,
  staking_pool,
  cover_asset,
  cover_amount,
  dollar_cover_amount,
  claim_amount,
  dollar_claim_amount,
  cover_start_time,
  cover_end_time,
  claim_submit_time,
  yes_votes,
  no_votes,
  yes_nxm_votes,
  no_nxm_votes
FROM
  v2_open_claims
ORDER BY
  claim_submit_time DESC