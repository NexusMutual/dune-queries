
# All data comes from the setClaimRewardDetail() in call data


select
  "claimid" as claimId,
  "tokens" * 1E-18 as nxm,
  SUM("tokens" * 1E-18) OVER(
    ORDER BY
      "call_block_time"
  ),
  "call_block_time" as date
from
  nexusmutual."ClaimsData_call_setClaimRewardDetail"
WHERE
  "percCA" > 0
  AND call_success = true