WITH
  v1_vote_count AS (
    SELECT DISTINCT
      claimId AS claim_id,
      CASE
        WHEN SUM(CAST(tokens AS DOUBLE) * 1E-18 * verdict) OVER (
          PARTITION BY
            claimId
        ) > 0 THEN 1
        ELSE -1
      END AS result,
      SUM(
        CASE
          WHEN verdict = 1 THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          claimId
      ) AS yes_votes,
      SUM(
        CASE
          WHEN verdict = -1 THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          claimId
      ) AS no_votes,
      SUM(
        CASE
          WHEN verdict = 1 THEN CAST(tokens AS DOUBLE) * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          claimId
      ) AS yes_nxm_votes,
      SUM(
        CASE
          WHEN verdict = -1 THEN CAST(tokens AS DOUBLE) * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          claimId
      ) AS no_nxm_votes
    FROM
      nexusmutual_ethereum.ClaimsData_evt_VoteCast
  ),
  v1_assessor_rewards AS (
    SELECT
      claimId AS claim_id,
      CAST(call_block_time AS TIMESTAMP) AS date,
      CAST(tokens AS DOUBLE) * 1E-18 AS total_nxm_rewards,
      yes_votes,
      no_votes,
      yes_nxm_votes,
      no_nxm_votes,
      result AS verdict,
      CASE
        WHEN result = 1 THEN yes_votes
        WHEN result = -1 THEN no_votes
        ELSE -1
      END AS winning_no_votes
    FROM
      nexusmutual_ethereum.ClaimsData_call_setClaimRewardDetail AS t
      INNER JOIN v1_vote_count ON v1_vote_count.claim_id = t.claimId
    WHERE
      call_success = TRUE
  ),
  v2_vote_count AS (
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
          WHEN accepted = true THEN CAST(stakedAmount AS DOUBLE) * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          assessmentId
      ) AS yes_nxm_votes,
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
  v2_assessor_rewards AS (
    SELECT
      assessment_id,
      CAST(call_block_time AS TIMESTAMP) AS date,
      CAST(output_totalRewardInNXM AS DOUBLE) * 1E-18 AS total_nxm_rewards,
      yes_votes,
      no_votes,
      yes_nxm_votes,
      no_nxm_votes,
      CASE
        WHEN yes_nxm_votes > no_nxm_votes THEN yes_votes
        WHEN yes_nxm_votes < no_nxm_votes THEN no_votes
        ELSE -1
      END AS winning_no_votes,
      CASE
        WHEN yes_nxm_votes > no_nxm_votes THEN 1
        WHEN yes_nxm_votes < no_nxm_votes THEN -1
        ELSE -1
      END AS verdict
    FROM
      nexusmutual_ethereum.Assessment_call_assessments AS t
      INNER JOIN v2_vote_count ON v2_vote_count.assessment_id = t._0
    WHERE
      call_success = true
  ),
  assessor_rewards AS (
    SELECT
      date,
      CAST(NULL AS UINT256) AS v1_claim_id,
      assessment_id AS v2_claim_id,
      total_nxm_rewards,
      winning_no_votes,
      total_nxm_rewards / winning_no_votes AS average_reward_per_vote
    FROM
      v2_assessor_rewards
    UNION
    SELECT
      date,
      claim_id AS v1_claim_id,
      CAST(NULL AS UINT256) AS v2_claim_id,
      total_nxm_rewards,
      winning_no_votes,
      total_nxm_rewards / winning_no_votes AS average_reward_per_vote
    FROM
      v1_assessor_rewards
  ),
  running_assessor_rewards AS (
    SELECT
      date,
      v1_claim_id,
      v2_claim_id,
      total_nxm_rewards,
      SUM(total_nxm_rewards) OVER (
        ORDER BY
          date
      ) AS total_running_assessor_rewards,
      AVG(average_reward_per_vote) OVER () AS total_average_reward_per_vote
    FROM
      assessor_rewards
  )
SELECT
  date,
  CAST(v1_claim_id AS UINT256) AS v1_claim_id,
  CAST(v2_claim_id AS UINT256) AS v2_claim_id,
  total_nxm_rewards,
  total_running_assessor_rewards,
  total_average_reward_per_vote,
  MAX(total_running_assessor_rewards) OVER () AS max_total_running_assessor_rewards
FROM
  running_assessor_rewards
WHERE
  date >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND date <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER by
  v1_claim_id,
  v2_claim_id