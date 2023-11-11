WITH book_value_eth AS (

    SELECT
        CAST(call_block_time AS TIMESTAMP) AS ts,
    FROM
    nexusmutual_ethereum.Ramm_call_getBookValue AS a

    
)