with

claims as (
  select
    cr.claimId as claim_id,
    cr.coverId as cover_id,
    cr.userAddress as claimant,
    from_unixtime(cr.dateSubmit) as submit_time,
    if(cr.claimId = 102, cast(10.43 as double), cast(cp.requestedPayoutAmount as double)) as partial_claim_amount
  from nexusmutual_ethereum.ClaimsData_evt_ClaimRaise cr
    left join nexusmutual_ethereum.Claims_call_submitPartialClaim cp on cr.coverId = cp.coverId
      and cr.evt_tx_hash = cp.call_tx_hash
      and cp.requestedPayoutAmount > 0
      and cp.call_success
)

select
  claim_id,
  cover_id,
  claimant,
  submit_time,
  submit_date,
  partial_claim_amount,
  claim_status
from (
  select
    c.claim_id,
    c.cover_id,
    c.claimant,
    c.submit_time,
    date_trunc('day', c.submit_time) as submit_date,
    c.partial_claim_amount,
    cs._stat as claim_status,
    row_number() over (partition by c.claim_id order by cs._stat desc) as rn
  from nexusmutual_ethereum.ClaimsData_call_setClaimStatus cs
    inner join claims c on cs._claimId = c.claim_id
  where cs.call_success
) t
where rn = 1
