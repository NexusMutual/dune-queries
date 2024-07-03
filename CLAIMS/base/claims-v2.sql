with

claim_evt as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    claimId as claim_id,
    coverId as cover_id,
    productId as product_id,
    user,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.IndividualClaims_evt_ClaimSubmitted
),

claim_data as (
  select
    call_block_time as block_time,
    call_block_number as block_number,
    coverId as cover_id,
    cast(json_query(output_claim, 'lax $.assessmentId') as int) as assessment_id,
    cast(json_query(output_claim, 'lax $.coverAsset') as int) as cover_asset,
    cast(json_query(output_claim, 'lax $.payoutRedeemed') as boolean) as is_payout_redeemed,
    requestedAmount as requested_amount,
    ipfsMetadata as ipfs_metadata,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_time, call_tx_hash order by call_trace_address desc) as rn
  from nexusmutual_ethereum.IndividualClaims_call_submitClaimFor
  where call_success
  union all
  select
    call_block_time as block_time,
    call_block_number as block_number,
    coverId as cover_id,
    cast(json_query(output_claim, 'lax $.assessmentId') as int) as assessment_id,
    cast(json_query(output_claim, 'lax $.coverAsset') as int) as cover_asset,
    cast(json_query(output_claim, 'lax $.payoutRedeemed') as boolean) as is_payout_redeemed,
    requestedAmount,
    ipfsMetadata,
    call_tx_hash as tx_hash,
    row_number() over (partition by call_block_time, call_tx_hash order by call_trace_address desc) as rn
  from nexusmutual_ethereum.IndividualClaims_call_submitClaim
  where call_success
)

select
  ce.block_time,
  ce.block_number,
  ce.claim_id,
  ce.cover_id,
  ce.product_id,
  ce.user,
  cd.assessment_id,
  case cd.cover_asset
    when 0 then 'ETH'
    when 1 then 'DAI'
    when 6 then 'USDC'
    else 'NA'
  end as cover_asset,
  cd.requested_amount / if(cd.cover_asset = 6, 1e6, 1e18) as requested_amount,
  cd.is_payout_redeemed,
  cd.ipfs_metadata,
  ce.tx_hash
from claim_evt ce
  inner join claim_data cd on ce.block_number = cd.block_number and ce.tx_hash = cd.tx_hash and ce.cover_id = cd.cover_id
where cd.rn = 1
