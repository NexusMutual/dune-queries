WITH
  claims as (
    SELECT
      "_claimId" as claim_id,
      "_coverId" as coverId,
      date '1970-01-01 00:00:00' + concat("_nowtime", ' second') :: interval as claim_submit_time
    from
      nexusmutual."ClaimsData_call_addClaim"
  ),
  status as (
    SELECT
      "_claimId" as claim_id,
      "_stat" as statusNo,
      "call_block_number" as blockNo
    from
      nexusmutual."ClaimsData_call_setClaimStatus"
  ),
  assessor_rewards as (
    SELECT
      "claimid" as claimId,
      "tokens" * 1E-18 as nxm_assessor_rewards
    from
      nexusmutual."ClaimsData_call_setClaimRewardDetail"
    WHERE
      call_success = true
  ),
  cover_details as (
    select
      "cid" as cover_id,
      "evt_block_time" as cover_start_time,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as cover_end_time,
      "scAdd" as productContract,
      "sumAssured" as sum_assured,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
  ),
  v1_product_info as (
    select
      "contract_address" as product_address,
      "syndicate" as syndicate,
      "name" as product_name,
      "type" as product_type
    from
      dune_user_generated.nexus_v1_product_info_view
  ),
  cover_product_info as (
    SELECT
      cover_id,
      cover_start_time,
      cover_end_time,
      sum_assured,
      cover_asset,
      syndicate,
      product_name,
      product_type
    from
      cover_details
      INNER JOIN v1_product_info ON v1_product_info.product_address = cover_details.productContract
  ),
  votes as (
    select
      "claimId" as claim_id,
      SUM(
        case
          when "verdict" = 1 then 1
          else 0
        end
      ) OVER (PARTITION BY "claimId") as vote_yes,
      SUM(
        case
          when "verdict" = -1 then 1
          else 0
        end
      ) OVER (PARTITION BY "claimId") as vote_no,
      SUM(
        case
          when "verdict" = 1 then "tokens" * 1E-18
          else 0
        end
      ) OVER (PARTITION BY "claimId") as nxm_vote_yes,
      SUM(
        case
          when "verdict" = -1 then "tokens" * 1E-18
          else 0
        end
      ) OVER (PARTITION BY "claimId") as nxm_vote_no,
      SUM("tokens") OVER (PARTITION BY "claimId") * 1E-18 as total_tokens
    FROM
      nexusmutual."ClaimsData_evt_VoteCast"
  ),
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
  ),
  votes_quorum as (
    SELECT
      DISTINCT COALESCE(votes.claim_id, quorum.claim_id) as claim_id,
      ca_nxm_quorum,
      mv_nxm_quorum,
      vote_yes,
      vote_no,
      nxm_vote_yes,
      nxm_vote_no,
      total_tokens
    FROM
      quorum
      FULL JOIN votes ON votes.claim_id = quorum.claim_id
  ),
  claimsStatus as(
    SELECT
      status.claim_id,
      claims.coverId,
      claims.claim_submit_time,
      status.statusNo,
      case
        when status.statusNo = 0 then '            CA Vote'
        when status.statusNo = 1 then '            Does not exist (was: CA Vote Denied, Pending Member Vote)'
        when status.statusNo = 2 then '            CA Vote Threshold not Reached Accept, Pending Member Vote'
        when status.statusNo = 3 then '            CA Vote Threshold not Reached Deny, Pending Member Vote'
        when status.statusNo = 4 then '             CA Consensus not reached Accept, Pending Member Vote'
        when status.statusNo = 5 then '            CA Consensus not reached Deny, Pending Member Vote'
        when status.statusNo = 6 then ' final  D1  CA Vote Denied'
        when status.statusNo = 7 then '        A1  CA Vote Accepted'
        when status.statusNo = 8 then '        A2  CA Vote no solution, MV Accepted'
        when status.statusNo = 9 then ' final  D2  CA Vote no solution, MV Denied'
        when status.statusNo = 10 then '        A3  CA Vote no solution (maj: accept), MV Nodecision'
        when status.statusNo = 11 then ' final  D3  CA Vote no solution (maj: denied), MV Nodecision'
        when status.statusNo = 12 then ' final      Claim Accepted Payout Pending'
        when status.statusNo = 13 then ' final      Claim Accepted No Payout'
        when status.statusNo = 14 then ' final      Claim Accepted Payout Done'
      end as status_string
    from
      status
      INNER JOIN claims ON status.claim_id = claims.claim_id
    ORDER BY
      claims.claim_id
  ),
  claimsStatusDetails as (
    SELECT
      cover_details.cover_id,
      claimsStatus.claim_id,
      claimsStatus.claim_submit_time,
      cover_details.cover_start_time,
      cover_details.cover_end_time,
      assessor_rewards.nxm_assessor_rewards as assessor_rewards,
      cover_details.cover_asset,
      cover_details.sum_assured
    from
      claimsStatus
      INNER JOIN cover_details ON cover_details.cover_id = claimsStatus.coverId
      INNER JOIN assessor_rewards ON assessor_rewards.claimId = claimsStatus.claim_id
  ),
  claims_status_details_votes as (
    SELECT
      DISTINCT claimsStatusDetails.cover_id,
      votes.claim_id,
      1 as product,
      1 as syndicate,
      claimsStatusDetails.cover_asset,
      claimsStatusDetails.sum_assured,
      claimsStatusDetails.cover_start_time,
      claimsStatusDetails.cover_end_time,
      claimsStatusDetails.claim_submit_time as claim_submit_time,
      votes_quorum.vote_yes,
      votes_quorum.vote_no,
      votes_quorum.nxm_vote_yes,
      votes_quorum.nxm_vote_no,
      votes_quorum.ca_nxm_quorum,
      votes_quorum.mv_nxm_quorum,
      votes_quorum.total_tokens,
      claimsStatusDetails.assessor_rewards,
      case
        when nxm_vote_yes > nxm_vote_no then 'APPROVED'
        ELSE 'DENIED'
      end as verdict
    from
      claimsStatusDetails
      INNER JOIN votes_quorum ON votes_quorum.claim_id = claimsStatusDetails.claim_id
  ),
  changes_config as (
    select
      "call_block_time" as date,
      case
        when code = '\x43414d494e565400' then val
      end as mintime_hrs,
      case
        when code = '\x43414d4158565400' then val
      end as maxTime_hrs
    from
      nexusmutual."ClaimsData_call_updateUintParameters"
    WHERE
      code in ('\x43414d4158565400', '\x43414d494e565400')
    UNION
    SELECT
      '2019-01-01 00:00' as date,
      12 as minTime_hrs,
      48 as maxTime_hrs
  ),
  config as (
    SELECT
      date,
      Coalesce(
        minTime_hrs,
        lag(minTime_hrs) OVER(
          ORDER BY
            date
        )
      ) as minTime_hrs,
      Coalesce(
        maxTime_hrs,
        lag(maxTime_hrs) OVER(
          ORDER BY
            date
        )
      ) as maxTime_hrs
    FROM
      changes_config
  ),
  config_joined_result as (
    SELECT
      *,
      row_number() OVER(
        PARTITION BY claim_id
        ORDER BY
          config.date DESC
      ) as x
    from
      claims_status_details_votes
      LEFT JOIN config ON claims_status_details_votes.claim_submit_time > config.date
  )
select
  cover_id,
  claim_id,
  product,
  syndicate,
  cover_asset,
  sum_assured,
  cover_start_time,
  cover_end_time,
  claim_submit_time,
  claim_submit_time + (interval '1 hour' * maxTime_hrs) as voting_expiry,
  vote_yes,
  vote_no,
  nxm_vote_yes,
  nxm_vote_no,
  ca_nxm_quorum,
  mv_nxm_quorum,
  total_tokens,
  assessor_rewards,
  verdict
from
  config_joined_result as t
where
  x = 1
  and claim_submit_time + (interval '1 hour' * maxTime_hrs) < NOW()