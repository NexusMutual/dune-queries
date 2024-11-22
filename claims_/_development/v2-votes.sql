SELECT
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