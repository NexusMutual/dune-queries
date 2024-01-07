select
  DISTINCT "_claimId" as claim_id,
  SUM(
    case
      when "_vote" = 1 then 1
      else 0
    end
  ) OVER (PARTITION BY "_claimId") as mv_vote_yes,
  SUM(
    case
      when "_vote" = -1 then 1
      else 0
    end
  ) OVER (PARTITION BY "_claimId") as mv_vote_no,
  SUM(
    case
      when "_vote" = 1 then "_tokens" * 1E-18
      else 0
    end
  ) OVER (PARTITION BY "_claimId") as mv_nxm_vote_yes,
  SUM(
    case
      when "_vote" = -1 then "_tokens" * 1E-18
      else 0
    end
  ) OVER (PARTITION BY "_claimId") as mv_nxm_vote_no,
  SUM("_tokens") OVER (PARTITION BY "_claimId") * 1E-18 as mv_total_tokens
FROM
  nexusmutual."ClaimsData_call_setClaimTokensMV"
WHERE
  nexusmutual."ClaimsData_call_setClaimTokensMV"."call_success" = true
ORDER BY
  claim_id