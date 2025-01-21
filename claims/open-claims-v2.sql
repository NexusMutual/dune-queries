with

covers as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    staking_pool,
    product_id,
    product_name,
    product_type,
    cover_asset,
    sum_assured
  from query_4599092 -- covers v2 - base ref (fallback query)
),

claims as (
  select
    submit_time,
    submit_date,
    claim_id,
    cover_id,
    product_id,
    assessment_id,
    cover_asset,
    requested_amount
  --from query_3894982 -- claims v2 base (fallback) query
  from nexusmutual_ethereum.claims_v2
),

vote_count as (
  select
    assessmentId as assessment_id,
    max(evt_block_time) as last_vote,
    sum(if(accepted = true, 1, 0)) as yes_votes,
    sum(if(accepted = false, 1, 0)) as no_votes,
    sum(if(accepted = true, stakedAmount / 1e18, 0)) as yes_nxm_votes,
    sum(if(accepted = false, stakedAmount / 1e18, 0)) as no_nxm_votes
  from nexusmutual_ethereum.Assessment_evt_VoteCast
  group by 1
),

assessments as (
  select assessment_id, assessor_rewards
  from (
    select
      _0 as assessment_id,
      output_totalRewardInNXM / 1e18 as assessor_rewards,
      row_number() over (partition by call_block_time, call_tx_hash, _0 order by call_trace_address desc) as rn
    from nexusmutual_ethereum.Assessment_call_assessments
    where call_success
  ) t
  where rn = 1
),

open_claims as (
  select
    c.submit_time,
    c.submit_date,
    c.cover_id,
    c.assessment_id,
    c.product_id,
    c.cover_asset,
    c.requested_amount,
    coalesce(vc.yes_votes, 0) as yes_votes,
    coalesce(vc.no_votes, 0) as no_votes,
    coalesce(vc.yes_nxm_votes, 0) as yes_nxm_votes,
    coalesce(vc.no_nxm_votes, 0) as no_nxm_votes
  from claims c
    inner join vote_count vc on c.assessment_id = vc.assessment_id
    left join assessments a on c.assessment_id = a.assessment_id
  where a.assessment_id is null
),

latest_prices as (
  select
    minute as block_date,
    symbol,
    price as usd_price
  from prices.usd_latest
  where (symbol = 'ETH' and blockchain is null and contract_address is null)
    or (symbol = 'DAI' and blockchain = 'ethereum' and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f)
    or (symbol = 'USDC' and blockchain = 'ethereum' and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48)
    or (symbol = 'cbBTC' and blockchain = 'ethereum' and contract_address = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf)
)

select
  oc.assessment_id,
  oc.cover_id,
  'PENDING ❓' as verdict,
  concat(
    '<a href="https://app.nexusmutual.io/claims/claim/claim-details?claimId=',
    cast(oc.assessment_id as varchar),
    '" target="_blank">',
    'link',
    '</a>'
  ) as url_link,
  c.product_name,
  c.product_type,
  c.staking_pool,
  c.cover_asset,
  c.sum_assured as cover_amount,
  c.sum_assured * p.usd_price as dollar_cover_amount,
  oc.requested_amount as claim_amount,
  oc.requested_amount * p.usd_price as dollar_claim_amount,
  c.cover_start_time,
  c.cover_end_time,
  oc.submit_time as claim_submit_time,
  oc.yes_votes,
  oc.no_votes,
  oc.yes_nxm_votes,
  oc.no_nxm_votes
from covers c
  inner join open_claims oc on c.cover_id = oc.cover_id and coalesce(c.product_id, oc.product_id) = oc.product_id
  inner join latest_prices p on oc.cover_asset = p.symbol
order by oc.submit_time desc
