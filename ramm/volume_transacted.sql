WITH
  volume_transacted AS (
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      CAST(ethIn AS DOUBLE) * 1E-18 AS eth_in,
      0 AS nxm_out,
      0 AS nxm_in,
      0 AS eth_out
    FROM
      nexusmutual_ethereum.Ramm_evt_EthSwappedForNxm
    UNION ALL
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      0 AS eth_in,
      CAST(nxmOut AS DOUBLE) * 1E-18 AS nxm_out,
      0 AS nxm_in,
      0 AS eth_out
    FROM
      nexusmutual_ethereum.Ramm_evt_EthSwappedForNxm
    UNION ALL
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      0 AS eth_in,
      0 AS nxm_out,
      CAST(nxmIn AS DOUBLE) * 1E-18 AS nxm_in,
      0 AS eth_out
    FROM
      nexusmutual_ethereum.Ramm_evt_NxmSwappedForEth
    UNION ALL
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      0 AS eth_in,
      0 AS nxm_out,
      0 AS nxm_in,
      CAST(ethOut AS DOUBLE) * 1E-18 AS eth_out
    FROM
      nexusmutual_ethereum.Ramm_evt_NxmSwappedForEth
  ),
  cummulative_volume_transacted AS (
    SELECT
      ts,
      SUM(eth_in) OVER (
        ORDER BY
          ts
      ) AS cummulative_eth_in,
      SUM(nxm_in) OVER (
        ORDER BY
          ts
      ) AS cummulative_nxm_in,
      SUM(eth_out) OVER (
        ORDER BY
          ts
      ) AS cummulative_eth_out,
      SUM(nxm_out) OVER (
        ORDER BY
          ts
      ) AS cummulative_nxm_out
    FROM
      volume_transacted
  )
SELECT DISTINCT
  ts,
  cummulative_eth_in,
  cummulative_eth_out,
  cummulative_nxm_out,
  cummulative_nxm_in
FROM
  cummulative_volume_transacted
WHERE
  ts >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND ts <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  ts DESC NULLS FIRST