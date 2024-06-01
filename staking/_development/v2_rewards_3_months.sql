WITH
  rewards AS (
    SELECT
      a.call_block_time as cover_start_time,
      date_add(
        'second',
        CAST(JSON_QUERY(a.params, 'lax $.period') AS INT),
        a.call_block_time
      ) as cover_end_time,
      CAST(JSON_QUERY(a.params, 'lax $.productId') AS INT) AS product_id,
      CAST(JSON_QUERY(a.params, 'lax $.period') AS INT) AS period,
      output_coverId as cover_id,
      CAST(amount AS UINT256) * 1E-18 as reward_amount_nxm,
      CAST(amount AS UINT256) * 1E-18 * 60.0 * 60.0 * 24.0 / CAST(JSON_QUERY(a.params, 'lax $.period') AS DOUBLE) as reward_amount_nxm_per_day,
      poolId as pool_id,
      CASE
        WHEN date_add(
          'second',
          CAST(JSON_QUERY(a.params, 'lax $.period') AS INT),
          a.call_block_time
        ) > NOW() THEN CAST(amount AS UINT256) * 1E-18 * (
          to_unixTime(NOW()) - to_unixTime(a.call_block_time)
        ) / CAST(JSON_QUERY(a.params, 'lax $.period') AS DOUBLE)
        ELSE CAST(amount AS UINT256) * 1E-18
      END AS rewards_in_last_three_months
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards as b ON b.call_tx_hash = a.call_tx_hash
    WHERE
      a.call_success
      and b.call_success
      AND a.call_trace_address IS NULL
      OR SLICE(
        b.call_trace_address,
        1,
        cardinality(a.call_trace_address)
      ) = a.call_trace_address
  ),
  v2_rewards_total_emitted AS (
    SELECT DISTINCT
      pool_id,
      SUM(rewards_in_last_three_months) OVER (
        PARTITION BY
          pool_id
      ) AS total_rewards
    FROM
      rewards
  )
SELECT
  *
FROM
  v2_rewards_total_emitted