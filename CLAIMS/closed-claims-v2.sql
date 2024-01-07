WITH
  v2_claims AS (
    SELECT
      coverId,
      ipfsMetadata,
      call_block_time AS claim_submit_time,
      CAST(
        JSON_QUERY(output_claim, 'lax $.assessmentId') AS INT
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
      coverId,
      ipfsMetadata,
      call_block_time AS claim_submit_time,
      CAST(
        JSON_QUERY(output_claim, 'lax $.assessmentId') AS INT
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
    select DISTINCT
      legacyProductId AS v1_product_address,
      output_0 AS v2_product_id
    from
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
    select
      cid AS v1_cover_id,
      evt_block_time AS cover_start_time,
      scAdd AS productContract,
      CAST(sumAssured AS DOUBLE) AS sum_assured,
      'v1' AS syndicate,
      product_name,
      product_type_name AS product_type,
      case
        when curr = 0x45544800 then 'ETH'
        when curr = 0x44414900 then 'DAI'
      end AS cover_asset,
      from_unixtime(CAST(expiry AS DOUBLE)) AS cover_end_time
    from
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
  vote_count AS (
    select DISTINCT
      assessmentId AS assessment_id,
      MAX(evt_block_time) OVER (
        PARTITION BY
          assessmentId
      ) AS last_vote,
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
    from
      nexusmutual_ethereum.Assessment_evt_VoteCast
  ),
  v2_assessor_rewards AS (
    SELECT DISTINCT
      _0 AS assessment_id,
      output_totalRewardInNXM * 1E-18 AS assessor_rewards
    FROM
      nexusmutual_ethereum.Assessment_call_assessments AS t
    WHERE
      t.call_success
  ),
  prices AS (
    SELECT DISTINCT
      DATE_TRUNC('day', minute) AS minute,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          symbol
        ,
          DATE_TRUNC('day', minute)
      ) AS price_dollar
    FROM
      prices.usd
    WHERE
      (
        symbol = 'DAI'
        OR symbol = 'ETH'
      )
      AND minute > CAST('2019-05-01 00:00:00' AS TIMESTAMP)
  ),
  covers_with_price AS (
    SELECT
      *
    FROM
      prices AS t
      INNER JOIN covers ON t.minute = DATE_TRUNC('day', covers.cover_start_time)
      AND t.symbol = covers.cover_asset
  ),
  completed_claims AS (
    SELECT DISTINCT
      claim_submit_time,
      CAST(coverId AS UINT256) AS cover_id,
      t.assessment_id AS assessment_id,
      amount_requested,
      cover_asset,
      COALESCE(yes_votes, 0) AS yes_votes,
      COALESCE(no_votes, 0) AS no_votes,
      COALESCE(yes_nxm_votes, 0) AS yes_nxm_votes,
      COALESCE(no_nxm_votes, 0) AS no_nxm_votes,
      COALESCE(assessor_rewards, 0) AS assessor_rewards,
      last_vote
    FROM
      v2_claims AS t
      LEFT JOIN vote_count ON CAST(vote_count.assessment_id AS INT) = CAST(t.assessment_id AS INT)
      LEFT JOIN v2_assessor_rewards ON CAST(v2_assessor_rewards.assessment_id AS INT) = t.assessment_id
  )
SELECT
  CAST(assessment_id AS BIGINT) AS assessment_id,
  covers_with_price.cover_id,
  CASE
    WHEN yes_nxm_votes > no_nxm_votes
    AND yes_nxm_votes > 0 then 'APPROVED ✅'
    ELSE 'DENIED ❌'
  END AS verdict,
  CONCAT(
    '<a href="https://app.nexusmutual.io/assessment/view-claim?claimId=',
    CAST(assessment_id AS VARCHAR),
    '" target="_blank">',
    'link',
    '</a>'
  ) AS url_link,
  product_name,
  product_type,
  syndicate,
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
  no_nxm_votes,
  CASE
    WHEN yes_nxm_votes > no_nxm_votes
    AND yes_nxm_votes > 0 THEN assessor_rewards / yes_votes
    WHEN no_nxm_votes > 0 THEN assessor_rewards / no_votes
    ELSE 0
  END AS assessor_rewards_per_vote,
  assessor_rewards,
  last_vote
FROM
  completed_claims
  INNER JOIN covers_with_price ON covers_with_price.cover_id = completed_claims.cover_id
  INNER JOIN prices AS s ON s.minute = DATE_TRUNC('day', completed_claims.claim_submit_time)
  AND s.symbol = completed_claims.cover_asset
WHERE
  (date_add('day', 3, claim_submit_time) <= NOW())
  AND (
    last_vote IS NULL
    OR date_add('day', 1, last_vote) <= NOW()
  )
ORDER BY
  claim_submit_time DESC