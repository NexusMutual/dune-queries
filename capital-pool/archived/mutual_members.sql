WITH
  kyc_verdict_counts AS (
    SELECT DISTINCT
      DATE_TRUNC('day', call_block_time) AS day,
      COUNT(*) OVER (
        PARTITION BY
          DATE_TRUNC('day', call_block_time)
      ) AS count_per_day
    FROM
      nexusmutual_ethereum.MemberRoles_call_kycVerdict
    WHERE
      call_success
      AND verdict
    UNION
    SELECT
      DATE_TRUNC('day', call_block_time) AS day,
      CARDINALITY(userArray) AS count_per_day
    FROM
      nexusmutual_ethereum.MemberRoles_call_addMembersBeforeLaunch
    WHERE
      call_success
    UNION ALL -- v2 join function
    SELECT DISTINCT
      DATE_TRUNC('day', call_block_time) AS day,
      COUNT(*) OVER (
        PARTITION BY
          DATE_TRUNC('day', call_block_time)
      ) AS count_per_day
    FROM
      nexusmutual_ethereum.MemberRoles_call_join
    WHERE
      call_success
  ),
  memebership_revoked AS (
    SELECT DISTINCT
      DATE_TRUNC('day', call_block_time) AS day,
      COUNT(*) OVER (
        PARTITION BY
          DATE_TRUNC('day', call_block_time)
      ) AS revoked_count
    FROM
      nexusmutual_ethereum.MemberRoles_call_withdrawMembership
    WHERE
      call_success = TRUE
  ),
  kyc_verdict_day_counts AS (
    SELECT DISTINCT
      COALESCE(kyc_verdict_counts.day, memebership_revoked.day) AS day,
      COALESCE(revoked_count, 0) AS revoked_count,
      COALESCE(count_per_day, 0) AS count_per_day
    FROM
      kyc_verdict_counts
      FULL JOIN memebership_revoked ON kyc_verdict_counts.day = memebership_revoked.day
  ),
  members AS (
    SELECT
      day,
      SUM(count_per_day - revoked_count) OVER (
        ORDER BY
          day NULLS FIRST
      ) AS running_member_count
    FROM
      kyc_verdict_day_counts
  )
SELECT
  *
FROM
  members
WHERE
  day >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND day <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  day DESC