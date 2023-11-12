WITH
  eth_released AS (
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      CAST(ethOut AS DOUBLE) * 1E-18 AS eth_out
    FROM
      nexusmutual_ethereum.QuotationData_evt_nxmswappedforeth
  )
SELECT
  *
FROM
  eth_released
ORDER BY
  ts DESC