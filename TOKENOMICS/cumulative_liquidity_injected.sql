WITH
  eth_injected AS (
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      CAST(injected AS DOUBLE) * 1E-18 AS eth_injected
    FROM
      nexusmutual_ethereum.QuotationData_evt_ethinjected
  )
SELECT
  *
FROM
  eth_injected
ORDER BY
  ts DESC