WITH
  vote_count AS (
    SELECT
      concat('V1_', CAST(claimId AS VARCHAR)) AS name,
      claimId AS v1_claim_id,
      NULL AS v2_claim_id,
      evt_tx_hash,
      CASE
        WHEN verdict = 1 THEN true
        WHEN verdict = -1 THEN false
      END AS result,
      CAST(tokens AS DOUBLE) * 1E-18 AS nxm_vote
    FROM
      nexusmutual_ethereum.ClaimsData_evt_VoteCast
    UNION
    SELECT
      concat('V2_', CAST(assessmentId AS VARCHAR)) AS name,
      NULL AS v1_claim_id,
      assessmentId AS v2_claim_id,
      evt_tx_hash,
      accepted,
      CAST(stakedAmount AS DOUBLE) * 1E-18 AS nxm_vote
    FROM
      nexusmutual_ethereum.Assessment_evt_VoteCast
  ),
  votes_with_gas_usage AS (
    SELECT
    DISTINCT
      name,
      v1_claim_id,
      v2_claim_id,
      CASE
        WHEN v1_claim_id IS NOT NULL THEN AVG(gas_used * gas_price * 1E-18) OVER (
          partition by
            name
        )
        ELSE NULL
      END AS v1_average_gas_cost,
      CASE
        WHEN v2_claim_id IS NOT NULL THEN AVG(gas_used * gas_price * 1E-18) OVER (
          partition by
            name
        )
        ELSE NULL
      END AS v2_average_gas_cost,
      CASE
        WHEN v1_claim_id IS NOT NULL THEN AVG(CAST(gas_used AS double)) OVER (
          PARTITION BY
            name
        )
        ELSE NULL
      END AS v1_gas_used,
      CASE
        WHEN v2_claim_id IS NOT NULL THEN AVG(CAST(gas_used AS double)) OVER (
          PARTITION BY
            name
        )
        ELSE NULL
      END AS v2_gas_used
    FROM
      vote_count
      INNER JOIN ethereum.transactions AS t ON vote_count.evt_tx_hash = t.hash
    WHERE
      block_time > CAST('2019-05-01' AS TIMESTAMP)
  )
SELECT
*,
AVG(v1_average_gas_cost) OVER () AS v1_average_gas_cost_all,
AVG(v2_average_gas_cost) OVER () AS v2_average_gas_cost_all
FROM
  votes_with_gas_usage
ORDER by
  v1_claim_id,
  v2_claim_id