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
  from query_4599092 -- covers v2 - base root (fallback query)
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
  from query_3894982 -- claims v2 base (fallback) query
  --from nexusmutual_ethereum.claims_v2
),

-- Claims Committee assessment start/end times (72 hour voting period)
assessment_started as (
  select
    claimId as claim_id,
    assessorGroupId as assessor_group_id,
    from_unixtime(cast("start" as double)) as voting_start_time,
    from_unixtime(cast("end" as double)) as voting_end_time,
    evt_block_time
  from nexusmutual_ethereum.assessments_evt_assessmentstarted
),

-- voting end can be extended
voting_end_extended as (
  select
    claimId as claim_id,
    from_unixtime(cast(newEnd as double)) as new_voting_end_time,
    evt_block_time,
    row_number() over (partition by claimId order by evt_block_time desc) as rn
  from nexusmutual_ethereum.assessments_evt_votingendchanged
),

-- Claims Committee votes (2 of 3 needed for approval)
vote_count as (
  select
    claimId as claim_id,
    min(evt_block_time) as first_vote_time,
    max(evt_block_time) as last_vote_time,
    sum(if(support = true, 1, 0)) as yes_votes,
    sum(if(support = false, 1, 0)) as no_votes
  from nexusmutual_ethereum.assessments_evt_votecast
  group by 1
),

-- combine assessment timing with extended end times
assessment_timing as (
  select
    a.claim_id,
    a.assessor_group_id,
    a.voting_start_time,
    coalesce(e.new_voting_end_time, a.voting_end_time) as voting_end_time
  from assessment_started a
    left join voting_end_extended e on a.claim_id = e.claim_id and e.rn = 1
),

-- open claims: voting end + 24h cool-down not yet passed
open_claims as (
  select
    c.submit_time,
    c.submit_date,
    c.cover_id,
    c.claim_id as assessment_id,
    c.product_id,
    c.cover_asset,
    c.requested_amount,
    coalesce(vc.yes_votes, 0) as yes_votes,
    coalesce(vc.no_votes, 0) as no_votes
  from claims c
    inner join assessment_timing at on c.claim_id = at.claim_id
    left join vote_count vc on c.claim_id = vc.claim_id
  where date_add('day', 1, at.voting_end_time) > now()
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
  order by 1 desc
  limit 1
)

select
  oc.assessment_id,
  oc.cover_id,
  'PENDING ❓' as verdict,
  concat(
    '<a href="https://app.nexusmutual.io/claims/details/',
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
  oc.no_votes
from covers c
  inner join open_claims oc on c.cover_id = oc.cover_id and coalesce(c.product_id, oc.product_id) = oc.product_id
  inner join latest_prices p on oc.cover_asset = p.symbol
order by oc.submit_time desc
