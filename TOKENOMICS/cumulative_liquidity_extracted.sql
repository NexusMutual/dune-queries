WITH
  eth_extracted AS (
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      CAST(extracted AS DOUBLE) * 1E-18 AS eth_extracted
    FROM
      nexusmutual_ethereum.QuotationData_evt_ethextracted
  )
SELECT
  *
FROM
  eth_extracted
ORDER BY
  ts DESC