WITH
  get_reserves AS (
    SELECT
      date_trunc('minute', call_block_time) AS ts,
      CAST(output_0 AS DOUBLE) AS eth_liquidity,
      CAST(output_3 AS DOUBLE) AS budget
    FROM
      nexusmutual_ethereum.Ramm_call_getReserves
  )
SELECT
  *
FROM
  get_reserves