with
  --- QUORUM CACULATION
  CA_token as (
    SELECT
      "claimId" as claim_id,
      case
        when "member" = 0 then "output_tokens"
      END as CA_tokens,
      "call_tx_hash" as ca_tx_hash,
      "call_trace_address" as ca_call_address
    FROM
      nexusmutual."Claims_call_getCATokens"
    where
      nexusmutual."Claims_call_getCATokens"."call_success" = true
      and "member" = 0
  ),
  -- join CA_tokens and MV_tokens to get all token quests
  MV_token as (
    select
      "claimId" as claim_id,
      case
        when "member" = 1 then "output_tokens"
      END as MV_tokens,
      "call_tx_hash" as mv_tx_hash,
      "call_trace_address" as mv_call_address
    FROM
      nexusmutual."Claims_call_getCATokens"
    where
      nexusmutual."Claims_call_getCATokens"."call_success" = true
      and "member" = 1
  ),
  mv_quorum as (
    select
      claim_id,
      MV_tokens * 5 / "output_tokenPrice" as mv_nxm_quorum,
      nexusmutual."Pool_call_getTokenPrice"."call_block_time" as mv_ts
    from
      nexusmutual."Pool_call_getTokenPrice"
      INNER JOIN MV_token ON MV_token.mv_tx_hash = nexusmutual."Pool_call_getTokenPrice"."call_tx_hash"
    where
      nexusmutual."Pool_call_getTokenPrice"."call_success" = true
      and MV_token.mv_call_address <@ nexusmutual."Pool_call_getTokenPrice"."call_trace_address" -- use the overlap function to check they both part of the same call tree
    UNION
    select
      claim_id,
      MV_tokens * 5 / "output_tokenPrice" as mv_nxm_quorum,
      nexusmutual."MCR_call_calculateTokenPrice"."call_block_time" as mv_ts
    from
      nexusmutual."MCR_call_calculateTokenPrice"
      INNER JOIN MV_token ON MV_token.mv_tx_hash = nexusmutual."MCR_call_calculateTokenPrice"."call_tx_hash"
    where
      nexusmutual."MCR_call_calculateTokenPrice"."call_success" = true
      and MV_token.mv_call_address <@ nexusmutual."MCR_call_calculateTokenPrice"."call_trace_address" -- use the overlap function to check they both part of the same call tree
  ),
  ca_quorum as (
    select
      claim_id,
      CA_tokens * 5 / "output_tokenPrice" as ca_nxm_quorum,
      nexusmutual."Pool_call_getTokenPrice"."call_block_time" as ca_ts
    from
      nexusmutual."Pool_call_getTokenPrice"
      INNER JOIN CA_token ON CA_token.ca_tx_hash = nexusmutual."Pool_call_getTokenPrice"."call_tx_hash"
    where
      nexusmutual."Pool_call_getTokenPrice"."call_success" = true
      and CA_token.ca_call_address <@ nexusmutual."Pool_call_getTokenPrice"."call_trace_address" -- use the overlap function to check they both part of the same call tree
    UNION
    select
      claim_id,
      CA_tokens * 5 / "output_tokenPrice" as ca_nxm_quorum,
      nexusmutual."MCR_call_calculateTokenPrice"."call_block_time" as ca_ts
    from
      nexusmutual."MCR_call_calculateTokenPrice"
      INNER JOIN CA_token ON CA_token.ca_tx_hash = nexusmutual."MCR_call_calculateTokenPrice"."call_tx_hash"
    where
      nexusmutual."MCR_call_calculateTokenPrice"."call_success" = true
      and CA_token.ca_call_address <@ nexusmutual."MCR_call_calculateTokenPrice"."call_trace_address" -- use the overlap function to check they both part of the same call tree
  ),
  quorum as (
    SELECT
      COALESCE(ca_quorum.claim_id, mv_quorum.claim_id) as claim_id,
      ca_nxm_quorum,
      ca_ts,
      mv_nxm_quorum,
      mv_ts
    FROM
      ca_quorum
      FULL JOIN mv_quorum ON ca_quorum.claim_id = mv_quorum.claim_id
  )
SELECT
  *
FROM
  quorum