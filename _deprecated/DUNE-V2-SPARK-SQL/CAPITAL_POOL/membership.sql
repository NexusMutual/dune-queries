WITH
  kyc_verdict_counts (
    SELECT
      date_trunc('day', call_block_time) AS day,
      *
    FROM
      nexusmutual_ethereum.MemberRoles_call_kycVerdict
    WHERE
      call_success = true
  ),
  kyc_verdict_day_counts AS (
    SELECT
      distinct day,
      COUNT(day) OVER (
        PARTITION BY
          day
      ) AS count_per_day
    FROM
      kyc_verdict_counts
  )
SElect
  day,
  SUM(count_per_day) OVER (
    ORDER BY
      day
  ) AS running_member_count
FROM
  kyc_verdict_day_counts