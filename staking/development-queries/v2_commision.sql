WITH
  commission AS (
    SELECT
      a.call_block_time as ts,
      poolId AS pool_id,
      productId AS product_id,
      output_premium * 1E-18 AS premium,
      period,
      output_coverId as cover_id,
      CAST(
        JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
      ) / 100 AS commission_ratio,
      CAST(
        JSON_QUERY(params, 'lax $.commissionRatio') AS DOUBLE
      ) * output_premium * 1E-18 / 10000 AS commission_distibutor_fee,
      JSON_QUERY(params, 'lax $.commissionDestination') AS commission_address
    FROM
      nexusmutual_ethereum.StakingProducts_call_getPremium AS a
      INNER JOIN nexusmutual_ethereum.Cover_call_buyCover AS b ON a.call_tx_hash = b.call_tx_hash
    WHERE
      a.call_success
      AND b.call_success
      AND a.contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4
  )
SELECT
  DISTINCT pool_id,
  SUM(commission_distibutor_fee) OVER (PARTITION BY pool_id) AS total_commission_pool_total
FROM
  commission