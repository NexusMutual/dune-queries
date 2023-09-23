WITH
  v1_claims AS (
    SELECT
      _claimId AS claim_id,
      _coverId AS cover_id,
      from_unixtime(CAST(_nowTime AS DOUBLE)) AS claim_submit_time,
      requestedPayoutAmount AS partial_claim_amount
    FROM
      nexusmutual_ethereum.ClaimsData_call_addClaim AS t
      LEFT JOIN nexusmutual_ethereum.Claims_call_submitPartialClaim AS v ON v.coverId = t._coverId
  ),
  status AS (
    SELECT
      claim_id,
      cover_id,
      call_block_time AS status_date,
      claim_submit_time,
      _stat AS status,
      partial_claim_amount
    FROM
      v1_claims AS t
      INNER JOIN nexusmutual_ethereum.ClaimsData_call_setClaimStatus AS s ON s._claimId = t.claim_id
    WHERE
      CAST(s._stat AS INT) = 14
  ),
  v1_claims_paid AS (
    SELECT
      claim_submit_time,
      status_date,
      COALESCE(
        CAST(partial_claim_amount AS DOUBLE),
        CAST(sumAssured AS DOUBLE)
      ) AS claim_amount,
      CASE
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
        ELSE 'NA'
      END AS cover_asset
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent AS t
      INNER JOIN status ON t.cid = status.cover_id
  ),
  v2_claims AS (
    SELECT
      call_block_time AS claim_submit_time,
      coverId,
      ipfsMetadata,
      CAST(
        JSON_QUERY(output_claim, 'lax $.assessmentId') AS INT
      ) AS assessment_id,
      CAST(
        JSON_QUERY(output_claim, 'lax $.amount') AS DOUBLE
      ) * 1E-18 AS amount,
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
      call_block_time AS claim_submit_time,
      coverId,
      ipfsMetadata,
      CAST(
        JSON_QUERY(output_claim, 'lax $.assessmentId') AS INT
      ) AS assessment_id,
      CAST(
        JSON_QUERY(output_claim, 'lax $.amount') AS DOUBLE
      ) * 1E-18 AS amount,
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
  v2_claims_paid AS (
    SELECT DISTINCT
      claim_submit_time,
      v2_claims.assessment_id,
      amount,
      cover_asset
    FROM
      nexusmutual_ethereum.IndividualClaims_call_redeemClaimPayout AS t
      INNER JOIN v2_claims ON CAST(t.claimId AS INT) = v2_claims.assessment_id
    WHERE
      t.call_success = true
  ),
  claims_paid AS (
    SELECT
      DATE_TRUNC('day', claim_submit_time) AS claim_time,
      amount,
      cover_asset
    FROM
      v2_claims_paid
    UNION
    SELECT
      DATE_TRUNC('day', claim_submit_time) AS claim_time,
      claim_amount AS amount,
      cover_asset
    FROM
      v1_claims_paid
    UNION
    SELECT
      CAST('2021-11-05 00:00' AS TIMESTAMP) AS claim_time,
      10.43 AS amount,
      'ETH' AS cover_asset
  ),
  day_prices AS (
    SELECT DISTINCT
      date_trunc('day', minute) AS time,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          date_trunc('day', minute),
          symbol
      ) AS price_dollar
    FROM
      prices.usd
    WHERE
      (
        symbol = 'DAI'
        OR symbol = 'ETH'
      )
      AND minute > CAST('2019-05-23' AS TIMESTAMP)
  ),
  eth_prices AS (
    SELECT
      time,
      price_dollar AS eth_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'ETH'
  ),
  dai_prices AS (
    SELECT
      time,
      price_dollar AS dai_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'DAI'
  ),
  prices AS (
    select
      eth_prices.time,
      eth_price_dollar,
      dai_price_dollar
    from
      eth_prices
      INNER JOIN dai_prices ON eth_prices.time = dai_prices.time
  ),
  total_claims AS (
    SELECT
      claim_time,
      SUM(
        CASE
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'USD' THEN amount * dai_price_dollar
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'ETH' THEN amount * dai_price_dollar / eth_price_dollar
          ELSE 0
        END
      ) OVER (
        ORDER BY
          claim_time
      ) AS running_dai_claimed,
      SUM(
        CASE
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'USD' THEN amount * eth_price_dollar
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'ETH' THEN amount
          ELSE 0
        END
      ) OVER (
        ORDER BY
          claim_time
      ) AS running_eth_claimed,
      SUM(
        CASE
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'USD' THEN amount * eth_price_dollar
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'ETH' THEN amount
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'USD' THEN amount * dai_price_dollar
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'ETH' THEN amount * dai_price_dollar / eth_price_dollar
          ELSE 0
        END
      ) OVER (
        ORDER BY
          claim_time
      ) AS running_total_claimed
    FROM
      claims_paid
      INNER JOIN prices ON claims_paid.claim_time = prices.time
  )
SELECT
  *
FROM
  total_claims
WHERE
  claim_time >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND claim_time <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  claim_time DESC