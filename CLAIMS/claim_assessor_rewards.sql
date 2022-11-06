select
  "claimid" as claimId,
  "tokens" * 1E-18 as nxm_assessor_rewards,
  SUM("tokens" * 1E-18) OVER(
    ORDER BY
      "call_block_time"
  ),
  "call_block_time" as date
from
  nexusmutual."ClaimsData_call_setClaimRewardDetail"
WHERE
  call_success = true