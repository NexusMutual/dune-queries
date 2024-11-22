CA_token /* QUORUM CACULATION */ AS (
  SELECT
    claimId AS claim_id,
    CASE
      WHEN member = 0 THEN output_tokens
    END AS CA_tokens,
    call_tx_hash AS ca_tx_hash,
    call_trace_address AS ca_call_address
  FROM
    nexusmutual_ethereum.Claims_call_getCATokens
  WHERE
    nexusmutual.Claims_call_getCATokens.call_success = TRUE
    AND member = 0
),
MV_token /* join CA_tokens and MV_tokens to get all token quests */ AS (
  SELECT
    claimId AS claim_id,
    CASE
      WHEN member = 1 THEN output_tokens
    END AS MV_tokens,
    call_tx_hash AS mv_tx_hash,
    call_trace_address AS mv_call_address
  FROM
    nexusmutual_ethereum.Claims_call_getCATokens
  WHERE
    nexusmutual.Claims_call_getCATokens.call_success = TRUE
    AND member = 1
),
mv_quorum AS (
  SELECT
    claim_id,
    output_tokenPrice AS mv_quorum_price,
    nexusmutual.Pool_call_getTokenPrice.call_block_time AS mv_ts
  FROM
    nexusmutual_ethereum.Pool_call_getTokenPrice
    INNER JOIN MV_token ON MV_token.mv_tx_hash = nexusmutual.Pool_call_getTokenPrice.call_tx_hash
  WHERE
    nexusmutual.Pool_call_getTokenPrice.call_success = TRUE
  UNION
  SELECT
    claim_id,
    output_tokenPrice AS mv_quorum_price,
    nexusmutual.MCR_call_calculateTokenPrice.call_block_time AS mv_ts
  FROM
    nexusmutual_ethereum.MCR_call_calculateTokenPrice
    INNER JOIN MV_token ON MV_token.mv_tx_hash = nexusmutual.MCR_call_calculateTokenPrice.call_tx_hash
  WHERE
    nexusmutual.MCR_call_calculateTokenPrice.call_success = TRUE
) /*      and MV_token.mv_call_address <@ nexusmutual.MCR_call_calculateTokenPrice.call_trace_address -- use the overlap function to check they both part of the same call tree */,
ca_quorum AS (
  SELECT
    claim_id,
    output_tokenPrice AS ca_quorum_price,
    nexusmutual.Pool_call_getTokenPrice.call_block_time AS ca_ts
  FROM
    nexusmutual_ethereum.Pool_call_getTokenPrice
    INNER JOIN CA_token ON CA_token.ca_tx_hash = nexusmutual.Pool_call_getTokenPrice.call_tx_hash
  WHERE
    nexusmutual.Pool_call_getTokenPrice.call_success = TRUE
  UNION
  SELECT
    claim_id,
    output_tokenPrice AS ca_quorum_price,
    nexusmutual.MCR_call_calculateTokenPrice.call_block_time AS ca_ts
  FROM
    nexusmutual_ethereum.MCR_call_calculateTokenPrice
    INNER JOIN CA_token ON CA_token.ca_tx_hash = nexusmutual.MCR_call_calculateTokenPrice.call_tx_hash
  WHERE
    nexusmutual.MCR_call_calculateTokenPrice.call_success = TRUE
)
SELECT
  COALESCE(ca_quorum.claim_id, mv_quorum.claim_id) AS claim_id,
  ca_quorum_price,
  ca_ts,
  mv_quorum_price,
  mv_ts
FROM
  ca_quorum
  FULL JOIN mv_quorum ON ca_quorum.claim_id = mv_quorum.claim_id