WITH
  eth_acquired AS (
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      CAST(ethIn AS DOUBLE) * 1E-18 AS eth_in
    FROM
      nexusmutual_ethereum.Ramm_evt_EthSwappedForNxm
  )
SELECT
  *
FROM
  eth_acquired
ORDER BY
  ts DESC