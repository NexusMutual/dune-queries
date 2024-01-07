WITH
  minted_nxm AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      SUM(CAST(value AS DOUBLE) / 1e18) AS NXM_supply_minted
    FROM
      nexusmutual_ethereum.NXMToken_evt_Transfer AS t
    WHERE
      t."from" = 0x0000000000000000000000000000000000000000
    GROUP BY
      1
    ORDER BY
      day
  ),
  burnt_nxm AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      SUM(-1 * CAST(value AS DOUBLE) / 1e18) AS NXM_supply_burnt
    FROM
      nexusmutual_ethereum.NXMToken_evt_Transfer AS t
    WHERE
      t."to" = 0x0000000000000000000000000000000000000000
    GROUP BY
      1
    ORDER BY
      day
  ),
  minted_burnt_nxm AS (
    SELECT
      CASE
        WHEN minted_nxm.day IS NULL THEN burnt_nxm.day
        ELSE minted_nxm.day
      END AS day,
      CASE
        WHEN NXM_supply_minted IS NULL THEN 0
        ELSE NXM_supply_minted
      END AS NXM_supply_minted,
      CASE
        WHEN NXM_supply_burnt IS NULL THEN 0
        ELSE NXM_supply_burnt
      END AS NXM_supply_burnt
    FROM
      minted_nxm
      FULL JOIN burnt_nxm ON minted_nxm.day = burnt_nxm.day
  )
SELECT
  day,
  SUM(NXM_supply_minted) OVER (
    ORDER BY
      day
  ) AS total_nxm_minted,
  SUM(NXM_supply_burnt) OVER (
    ORDER BY
      day
  ) AS total_nxm_burned,
  SUM(NXM_supply_minted + NXM_supply_burnt) OVER (
    ORDER BY
      day
  ) AS total_nxm
FROM
  minted_burnt_nxm
WHERE
  day >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND day <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  day DESC NULLS FIRST