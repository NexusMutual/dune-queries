WITH
  eth_extracted AS (
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      CAST(value AS DOUBLE) * 1E-18 AS eth_extracted
    FROM
      nexusmutual_ethereum.Ramm_evt_EthExtracted
  )
SELECT
  *
FROM
  eth_extracted
ORDER BY
  ts DESC