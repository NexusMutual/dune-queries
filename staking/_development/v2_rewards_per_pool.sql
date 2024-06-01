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
      date_diff('second', a.call_block_time, NOW()) AS secs_active,
      CASE
        WHEN date_add(
          'second',
          CAST(JSON_QUERY(a.params, 'lax $.period') AS INT),
          a.call_block_time
        ) < NOW() THEN 0
        ELSE CAST(amount AS DOUBLE) * 1E-18 * 60.0 * 60.0 * 24.0 / CAST(JSON_QUERY(a.params, 'lax $.period') AS DOUBLE)
      END as reward_amount_nxm_scaled
    FROM
      nexusmutual_ethereum.Cover_call_buyCover as a
      INNER JOIN nexusmutual_ethereum.TokenController_call_mintStakingPoolNXMRewards as b ON b.call_tx_hash = a.call_tx_hash
    WHERE
      a.call_trace_address IS NULL
      OR SLICE(
        b.call_trace_address,
        1,
        cardinality(a.call_trace_address)
      ) = a.call_trace_address