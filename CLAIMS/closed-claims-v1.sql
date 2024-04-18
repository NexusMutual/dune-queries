WITH
  prices AS (
     SELECT DISTINCT
      DATE_TRUNC('day', minute) AS minute,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          symbol,
          DATE_TRUNC('day', minute)
      ) AS price_dollar
    FROM prices.usd
    WHERE symbol IN ('DAI', 'ETH')
      AND coalesce(blockchain, 'ethereum') = 'ethereum'
      AND minute > CAST('2019-05-01 00:00:00' AS TIMESTAMP)
  ),
  cover_details AS (
    SELECT
      cid AS cover_id,
      evt_block_time AS cover_start_time,
      from_unixtime(CAST(expiry AS DOUBLE)) AS cover_end_time,
      scAdd AS productContract,
      sumAssured AS sum_assured,
      CASE
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS cover_asset,
      COALESCE(syndicate, 'v1') AS syndicate,
      COALESCE(product_name, 'unknown') AS product_name,
      COALESCE(product_type, 'unknown') AS product_type
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent AS t
      LEFT JOIN nexusmutual_ethereum.product_information AS s ON s.product_contract_address = t.scAdd
  ),
  partial_claim AS (
    SELECT
      coverId AS cover_id,
      requestedPayoutAmount AS partial_claim_amount,
      call_tx_hash
    FROM
      nexusmutual_ethereum.Claims_call_submitPartialClaim
    WHERE
      call_success = true
      AND requestedPayoutAmount > CAST(0 AS UINT256)
  ),
  claims AS (
    SELECT
      t._claimId AS claim_id,
      t._coverId AS cover_id,
      from_unixtime(CAST(_nowtime AS DOUBLE)) AS claim_submit_time,
      CASE
        WHEN t._claimId = CAST(102 AS UINT256) THEN CAST(10.43 AS DOUBLE)
        ELSE CAST(partial_claim_amount AS DOUBLE)
      END AS partial_claim_amount
    FROM
      nexusmutual_ethereum.ClaimsData_call_addClaim AS t
      LEFT JOIN partial_claim AS s ON s.cover_id = t._coverId
      AND s.call_tx_hash = t.call_tx_hash
    WHERE
      t.call_success = true
  ),
  claims_status AS (
    SELECT DISTINCT
      s.claim_id,
      MAX(_stat) OVER (
        PARTITION BY
          claim_id
      ) AS max_status_no,
      cover_id,
      claim_submit_time,
      partial_claim_amount
     FROM
      nexusmutual_ethereum.ClaimsData_call_setClaimStatus AS t
      INNER JOIN claims AS s ON t._claimId = s.claim_id
    WHERE
      t.call_success = true
  ),
  assessor_rewards AS (
    SELECT
      claimid AS claimId,
      tokens * 1E-18 AS nxm_assessor_rewards
     FROM
      nexusmutual_ethereum.ClaimsData_call_setClaimRewardDetail
    WHERE
      call_success = true
  ),
  -- CA & MV vote calculation
  ca_votes AS (
    SELECT DISTINCT
      _claimId AS claim_id,
      SUM(
        CASE
          WHEN _vote = 1 THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS ca_vote_yes,
      SUM(
        CASE
          WHEN _vote = -1 THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS ca_vote_no,
      SUM(
        CASE
          WHEN _vote = 1 THEN _tokens * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS ca_nxm_vote_yes,
      SUM(
        CASE
          WHEN _vote = -1 THEN _tokens * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS ca_nxm_vote_no,
      SUM(_tokens) OVER (
        PARTITION BY
          _claimId
      ) * 1E-18 AS ca_total_tokens
    FROM
      nexusmutual_ethereum.ClaimsData_call_setClaimTokensCA AS t
    WHERE
      t.call_success = TRUE
    ORDER BY
      claim_id
  ),
  mv_votes AS (
    SELECT DISTINCT
      _claimId AS claim_id,
      SUM(
        CASE
          WHEN _vote = 1 THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS mv_vote_yes,
      SUM(
        CASE
          WHEN _vote = -1 THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS mv_vote_no,
      SUM(
        CASE
          WHEN _vote = 1 THEN _tokens * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS mv_nxm_vote_yes,
      SUM(
        CASE
          WHEN _vote = -1 THEN _tokens * 1E-18
          ELSE 0
        END
      ) OVER (
        PARTITION BY
          _claimId
      ) AS mv_nxm_vote_no,
      SUM(_tokens) OVER (
        PARTITION BY
          _claimId
      ) * 1E-18 AS mv_total_tokens
    FROM
      nexusmutual_ethereum.ClaimsData_call_setClaimTokensMV AS t
    WHERE
      t.call_success = TRUE
    ORDER BY
      claim_id
  ),
  votes AS (
    SELECT
      COALESCE(ca_votes.claim_id, mv_votes.claim_id) AS claim_id,
      ca_vote_yes,
      ca_vote_no,
      ca_nxm_vote_yes,
      ca_nxm_vote_no,
      ca_total_tokens,
      mv_vote_yes,
      mv_vote_no,
      mv_nxm_vote_yes,
      mv_nxm_vote_no,
      mv_total_tokens
    FROM
      ca_votes
      FULL JOIN mv_votes ON ca_votes.claim_id = mv_votes.claim_id
  ),
  CA_token /* QUORUM CACULATION */ AS (
    SELECT
      claimId AS claim_id,
      CASE
        WHEN CAST(member AS INT) = 0 THEN output_tokens
      END AS CA_tokens,
      call_tx_hash AS ca_tx_hash,
      call_trace_address AS ca_call_address
    FROM
      nexusmutual_ethereum.Claims_call_getCATokens AS t
    WHERE
      t.call_success = TRUE
      AND CAST(member AS INT) = 0
  ),
  MV_token /* join CA_tokens and MV_tokens to get all token quests */ AS (
    SELECT
      claimId AS claim_id,
      CASE
        WHEN CAST(member AS INT) = 1 THEN output_tokens
      END AS MV_tokens,
      call_tx_hash AS mv_tx_hash,
      call_trace_address AS mv_call_address
    FROM
      nexusmutual_ethereum.Claims_call_getCATokens AS t
    WHERE
      t.call_success = TRUE
      AND CAST(member AS INT) = 1
  ),
  mv_quorum AS (
    SELECT
      claim_id,
      output_tokenPrice AS mv_quorum_price,
      t.call_block_time AS mv_ts
    FROM
      nexusmutual_ethereum.Pool_call_getTokenPrice AS t
      RIGHT JOIN MV_token ON MV_token.mv_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, MV_token.mv_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
    UNION
    SELECT
      claim_id,
      output_tokenPrice AS mv_quorum_price,
      t.call_block_time AS mv_ts
    FROM
      nexusmutual_ethereum.MCR_call_calculateTokenPrice AS t
      RIGHT JOIN MV_token ON MV_token.mv_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, MV_token.mv_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
  ),
  ca_quorum AS (
    SELECT
      claim_id,
      output_tokenPrice AS ca_quorum_price,
      t.call_block_time AS ca_ts
    FROM
      nexusmutual_ethereum.Pool_call_getTokenPrice AS t
      RIGHT JOIN CA_token ON CA_token.ca_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, CA_token.ca_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
    UNION
    SELECT
      claim_id,
      output_tokenPrice AS ca_quorum_price,
      t.call_block_time AS ca_ts
    FROM
      nexusmutual_ethereum.MCR_call_calculateTokenPrice AS t
      RIGHT JOIN CA_token ON CA_token.ca_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, CA_token.ca_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
  ),
  quorum AS (
    SELECT DISTINCT
      COALESCE(ca_quorum.claim_id, mv_quorum.claim_id) AS claim_id,
      ca_quorum_price,
      ca_ts,
      mv_quorum_price,
      mv_ts
    FROM
      ca_quorum
      FULL JOIN mv_quorum ON ca_quorum.claim_id = mv_quorum.claim_id
  ),
  votes_quorum AS (
    SELECT DISTINCT
      COALESCE(votes.claim_id, quorum.claim_id) AS claim_id,
      ca_vote_yes,
      ca_vote_no,
      ca_nxm_vote_yes,
      ca_nxm_vote_no,
      ca_total_tokens,
      ca_quorum_price,
      mv_vote_yes,
      mv_vote_no,
      mv_nxm_vote_yes,
      mv_nxm_vote_no,
      mv_total_tokens,
      mv_quorum_price
    FROM
      quorum
      FULL JOIN votes ON votes.claim_id = quorum.claim_id
  ),
  claims_status_details AS (
    SELECT
      s.cover_id,
      claim_id,
      claim_submit_time,
      s.cover_start_time,
     s.cover_end_time,
      assessor_rewards.nxm_assessor_rewards AS assessor_rewards,
      s.cover_asset,
      s.syndicate,
      s.product_name,
      s.product_type,
      max_status_no,
      COALESCE(
        CAST(partial_claim_amount AS DOUBLE),
        CAST(sum_assured AS DOUBLE)
      ) AS claim_amount,
      claim_t.price_dollar * COALESCE(
        CAST(partial_claim_amount AS DOUBLE),
        CAST(sum_assured AS DOUBLE)
      ) AS dollar_claim_amount,
      s.sum_assured,
      cover_t.price_dollar * s.sum_assured AS dollar_sum_assured
     FROM
      claims_status AS t
      LEFT JOIN cover_details AS s ON s.cover_id = t.cover_id
      LEFT JOIN assessor_rewards ON assessor_rewards.claimId = t.claim_id
      INNER JOIN prices AS claim_t ON claim_t.minute = DATE_TRUNC('day', t.claim_submit_time)
      AND claim_t.symbol = s.cover_asset
      INNER JOIN prices AS cover_t ON cover_t.minute = DATE_TRUNC('day', s.cover_start_time)
      AND cover_t.symbol = s.cover_asset
    WHERE
      max_status_no IS NOT NULL
      AND CAST(max_status_no AS INT) IN (6, 9, 11, 12, 13, 14) -- only get final status's
  ),
  claims_status_details_votes AS (
    SELECT
      CAST(claims_status_details.claim_id AS BIGINT) AS claim_id,
      claims_status_details.cover_id,
      case
      -- clim id 108 is a snapshot vote that was accepted
        when CAST(max_status_no AS INT) IN (12, 13, 14)
        OR claims_status_details.claim_id = CAST(102 AS UINT256) then 'ACCEPTED ✅'
        ELSE 'DENIED ❌'
      end AS verdict,
      CONCAT(
        '<a href="https://app.nexusmutual.io/claim-assessment/view-claim?claimId=',
        CAST(claims_status_details.claim_id AS VARCHAR),
        '" target="_blank">',
        'link',
        '</a>'
      ) AS url_link,
      claims_status_details.product_name,
      claims_status_details.product_type,
      claims_status_details.syndicate AS staking_pool,
      claims_status_details.cover_asset,
      sum_assured,
      dollar_sum_assured,
      claim_amount,
      dollar_claim_amount,
      claims_status_details.cover_start_time,
      claims_status_details.cover_end_time,
      claims_status_details.claim_submit_time AS claim_submit_time,
      votes_quorum.ca_vote_yes,
      votes_quorum.ca_vote_no,
      votes_quorum.ca_nxm_vote_yes,
      votes_quorum.ca_nxm_vote_no,
      votes_quorum.ca_vote_yes + votes_quorum.ca_vote_no AS ca_total_votes,
      votes_quorum.ca_total_tokens,
      claims_status_details.assessor_rewards / (
        votes_quorum.ca_vote_yes + votes_quorum.ca_vote_no
      ) AS nxm_assessor_rewards_per_vote,
      claims_status_details.sum_assured * 5 * 1E18 / votes_quorum.ca_quorum_price AS ca_nxm_quorum,
      votes_quorum.mv_vote_yes,
      votes_quorum.mv_vote_no,
      votes_quorum.mv_vote_yes + votes_quorum.mv_vote_no AS mv_total_votes,
      votes_quorum.mv_nxm_vote_yes,
      votes_quorum.mv_nxm_vote_no,
      votes_quorum.mv_total_tokens,
      claims_status_details.sum_assured * 5 * 1E18 / votes_quorum.mv_quorum_price AS mv_nxm_quorum,
      claims_status_details.assessor_rewards,
      case
        when ca_vote_yes > ca_vote_no THEN assessor_rewards / ca_vote_yes
        when ca_vote_yes < ca_vote_no THEN assessor_rewards / ca_vote_no
        ELSE 0
      end AS nxm_ca_assessor_rewards_per_vote,
      case
        when mv_vote_yes > mv_vote_no THEN assessor_rewards / mv_vote_yes
        when mv_vote_yes < mv_vote_no THEN assessor_rewards / mv_vote_no
        ELSE 0
      end AS nxm_mv_assessor_rewards_per_vote
     FROM
      claims_status_details
      LEFT JOIN votes_quorum ON votes_quorum.claim_id = claims_status_details.claim_id
  )
SELECT
  *
FROM
  claims_status_details_votes
ORDER BY
  claim_id DESC