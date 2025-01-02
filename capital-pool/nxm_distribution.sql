WITH
  nxm_sent AS (
    SELECT
     "from" AS address,
      SUM(-1 * CAST(value AS DOUBLE) * 1E-18) AS total_sent
    FROM
      erc20_ethereum.evt_Transfer AS t
    WHERE
      CAST(value AS DOUBLE) > 0
      AND contract_address  = 0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b
    GROUP BY
      1
  ),
  nxm_recv AS (
    SELECT
      "to" AS address,
      SUM(CAST(value AS DOUBLE) * 1E-18) AS total_recv
    FROM
     erc20_ethereum.evt_Transfer AS t
    WHERE
      CAST(value AS DOUBLE) > 0
      AND contract_address = 0xd7c49cee7e9188cca6ad8ff264c1da2e69d4cf3b
    GROUP BY
      1
  )
SELECT
  COALESCE(nxm_recv.address, nxm_sent.address) AS user_address,
  COALESCE(nxm_sent.total_sent, 0),
  COALESCE(nxm_recv.total_recv, 0),
  COALESCE(nxm_sent.total_sent, 0) + COALESCE(nxm_recv.total_recv, 0) AS total
FROM
  nxm_sent
  FULL JOIN nxm_recv ON nxm_recv.address = nxm_sent.address
WHERE
  COALESCE(nxm_sent.total_sent, 0) + COALESCE(nxm_recv.total_recv, 0) > 1E-12
ORDER BY
  total DESC