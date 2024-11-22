WITH
  eth_injected AS (
    SELECT
      CAST(evt_block_time AS TIMESTAMP) AS ts,
      CAST(value AS DOUBLE) * 1E-18 AS eth_injected
    FROM
      nexusmutual_ethereum.Ramm_evt_EthInjected
  ),
  cumulative_injected AS (
    SELECT
      ts,
      eth_injected,
      SUM(eth_injected) OVER (
        ORDER BY
          ts
      ) AS cummulative_injected
    FROM
      eth_injected
  )
SELECT
  ts,
  eth_injected,
  cummulative_injected,
  CAST(43850.0 AS DOUBLE) - cummulative_injected AS remaining_budget
FROM
  cumulative_injected
WHERE
  ts >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND ts <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  ts DESC