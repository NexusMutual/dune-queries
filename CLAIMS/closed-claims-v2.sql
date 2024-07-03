with

covers as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    syndicate,
    product_name,
    product_type,
    cover_asset,
    sum_assured
  --from query_3788370 -- covers v2 base (fallback) query
  from nexusmutual_ethereum.covers_v2
),

claims as (
  select
    claim_id,
    cover_id,
    product_id,
    cover_asset,
    requested_amount
  from query_3894982 -- claims v2 base (fallback) query
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
  from nexusmutual_ethereum.Assessment_evt_VoteCast
),

v2_assessor_rewards AS (
  SELECT DISTINCT
    _0 AS assessment_id,
    output_totalRewardInNXM * 1E-18 AS assessor_rewards
  FROM nexusmutual_ethereum.Assessment_call_assessments AS t
  WHERE t.call_success
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
  FROM prices.usd
  WHERE minute > CAST('2019-05-01' AS TIMESTAMP)
    and ((symbol = 'ETH' and blockchain is null)
      or (symbol = 'DAI' and blockchain = 'ethereum'))
),

covers_with_price AS (
  SELECT
    *
  FROM prices AS t
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
  FROM v2_claims AS t
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
FROM completed_claims
  INNER JOIN covers_with_price ON covers_with_price.cover_id = completed_claims.cover_id
  INNER JOIN prices AS s ON s.minute = DATE_TRUNC('day', completed_claims.claim_submit_time)
  AND s.symbol = completed_claims.cover_asset
WHERE (date_add('day', 3, claim_submit_time) <= NOW())
  AND (
    last_vote IS NULL
    OR date_add('day', 1, last_vote) <= NOW()
  )
ORDER BY claim_submit_time DESC
