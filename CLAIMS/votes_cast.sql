WITH
  votes as (
    select
      *,
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
  )
SELECT
  *
FROM
  votes