WITH
  volume_transacted AS (
    SELECT
      CAST(call_block_time AS TIMESTAMP) as ts,
      CAST(ethIn AS DOUBLE) * 1E-18 AS eth_in,
      0 AS nxm_out,
      0 AS nxm_in,
      0 AS eth_out
    FROM
      nexusmutual_ethereum.QuotationData_evt_ethswappedfornxm
    UNION ALL
    SELECT
      CAST(call_block_time AS TIMESTAMP) as ts,
      0 AS eth_in,
      CAST(nxm_out AS DOUBLE) * 1E-18 AS nxm_out,
      0 AS nxm_in,
      0 AS eth_out
    FROM
      nexusmutual_ethereum.QuotationData_evt_ethswappedfornxm
    UNION ALL
    SELECT
      CAST(call_block_time AS TIMESTAMP) as ts,
      0 AS eth_in,
      0 AS nxm_out,
      CAST(nxmIn AS DOUBLE) * 1E-18 AS nxm_in,
      0 AS eth_out
    FROM
      nexusmutual_ethereum.QuotationData_evt_nxmswappedforeth
    UNION ALL
    SELECT
      CAST(call_block_time AS TIMESTAMP) as ts,
      0 AS eth_in,
      0 AS nxm_out,
      0 AS nxm_in,
      CAST(ethOut AS DOUBLE) * 1E-18 AS eth_out
    FROM
      nexusmutual_ethereum.QuotationData_evt_nxmswappedforeth
  ),
  cummulative_volume_transacted AS (
    SELECT
      CAST(call_block_time AS TIMESTAMP) as ts,
      SUM(eth_in) OVER (
        ORDER BY
          ts
      ) as cummulative_eth_in,
      SUM(nxm_in) OVER (
        ORDER BY
          ts
      ) as cummulative_nxm_in,
      SUM(eth_out) OVER (
        ORDER BY
          ts
      ) as cummulative_eth_out,
      SUM(nxm_out) OVER (
        ORDER BY
          ts
      ) as cummulative_nxm_out
    FROM
      volume_transacted
  )
SELECT
  *
FROM
  cummulative_volume_transacted
WHERE
  day >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND day <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  day DESC NULLS FIRST