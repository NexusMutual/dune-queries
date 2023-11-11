WITH
    spot_price AS (
        SELECT
            CAST(call_block_time AS TIMESTAMP) AS ts,
             CAST(output_0 AS DOUBLE) / CAST(output_1 AS DOUBLE) AS spot_price_eth_a,
             CAST(output_0 AS DOUBLE) / CAST(output_2 AS DOUBLE) AS spot_price_eth_b
        FROM
            nexusmutual_ethereum.Ramm_call_getReserves
    )