WITH
  eth_liquidity AS (
    SELECT
      date_trunc('minute', call_block_time) AS ts,
      CAST(output_0 AS DOUBLE) AS eth_liquidity
    FROM
      nexusmutual_ethereum.Ramm_call_getReserves
  )
SELECT
*
FROM
eth_liquidity 