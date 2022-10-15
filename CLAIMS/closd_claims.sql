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
  cover_details as (
    select
      "cid" as cover_id,
      "evt_block_time" as cover_start_time,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as cover_end_time,
      "sumAssured" as sum_assured,
      "sumAssured" * 0.2 as assessor_rewards,
      case
        when "curr" = '\x45544800' then 'ETH'
        when "curr" = '\x44414900' then 'DAI'
      end as cover_asset,
      date '1970-01-01 00:00:00' + concat("expiry", ' second') :: interval as end_time
    from
      nexusmutual."QuotationData_evt_CoverDetailsEvent"
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
      cover_details.sum_assured,
      cover_details.assessor_rewards,
      cover_details.cover_asset
    from
      claimsStatus
      INNER JOIN cover_details ON cover_details.cover_id = claimsStatus.coverId
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
      1 as voting_expiry,
      votes.vote_yes,
      votes.vote_no,
      votes.nxm_vote_yes,
      votes.nxm_vote_no,
      votes.total_tokens,
      claimsStatusDetails.assessor_rewards,
      case
        when nxm_vote_yes > nxm_vote_no then 'PAID'
        ELSE 'REJECTED'
      end
    from
      claimsStatusDetails
      INNER JOIN votes ON votes.claim_id = claimsStatusDetails.claim_id
  )
SELECT
  *
from
  claims_status_details_votes