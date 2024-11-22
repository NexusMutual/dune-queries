WITH
  nxm_distribution AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      "to" AS address,
      CAST(value AS DOUBLE) * 1E-18 AS value
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      CAST(value AS DOUBLE) > 0
      AND "contract_address" = 0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b
    UNION ALL
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      "from" AS address,
      CAST(value AS DOUBLE) * -1E-18 AS value
    FROM
      erc20_ethereum.evt_Transfer
    WHERE
      CAST(value AS DOUBLE) > 0
      AND "contract_address" = 0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b
  ),
  nxm_running_total AS (
    SELECT DISTINCT
      day,
      address,
      SUM(CAST(value AS DOUBLE)) OVER (
        PARTITION BY
          address
        ORDER BY
          day
      ) AS running_total
    FROM
      nxm_distribution
    WHERE
      cast(address AS VARBINARY) != 0x0000000000000000000000000000000000000000
  ),
  exit_entrance AS (
    SELECT
      *,
      CASE
        when running_total <= 1E-12 then -1
        ELSE 0
      END AS exited_mututal,
      CASE
        WHEN running_total >= 1E-12 THEN 1
        ELSE 0
      END AS in_mututal
    FROM
      nxm_running_total
  ),
  unqiue_addresses AS (
    SELECT
      day,
      address,
      running_total,
      exited_mututal,
      in_mututal,
      CASE
        WHEN exited_mututal - COALESCE(
          LAG(exited_mututal) OVER (
            PARTITION BY
              address
            ORDER BY
              day
          ),
          -1
        ) = -1 THEN -1
        WHEN in_mututal - COALESCE(
          LAG(in_mututal) OVER (
            PARTITION BY
              address
            ORDER BY
              day
          ),
          0
        ) = 1 THEN 1
      END AS count_,
      CASE
        WHEN exited_mututal - COALESCE(
          LAG(exited_mututal) OVER (
            PARTITION BY
              address
            ORDER BY
              day
          ),
          -1
        ) = -1 THEN -1
      END AS exited,
      CASE
        WHEN in_mututal - COALESCE(
          LAG(in_mututal) OVER (
            PARTITION BY
              address
            ORDER BY
              day
          ),
          0
        ) = 1 THEN 1
      END AS entered
    FROM
      exit_entrance
  ),
  entered_and_exited AS (
    SELECT DISTINCT
      day,
      COALESCE(
        SUM(exited) OVER (
          PARTITION BY
            day
        ),
        0
      ) AS exited_per_day,
      COALESCE(
        SUM(entered) OVER (
          PARTITION BY
            day
        ),
        0
      ) AS entered_per_day
    FROM
      unqiue_addresses
  ),
  nxm_holders AS (
    SELECT
      day,
      exited_per_day,
      entered_per_day,
      entered_per_day + exited_per_day AS net_change,
      SUM(entered_per_day + exited_per_day) OVER (
        ORDER BY
          day
      ) AS running_unique_users
    FROM
      entered_and_exited
  )
SELECT
  *
FROM
  nxm_holders
WHERE
  day >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND day <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  day DESC NULLS FIRST