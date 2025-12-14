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

-- v1 assessment: stake-weighted voting (assessment_id <= 28)
vote_count_v1 as (
  select
    assessmentId as assessment_id,
    min(evt_block_time) as first_vote_time,
    max(evt_block_time) as last_vote_time,
    sum(if(accepted = true, 1, 0)) as yes_votes,
    sum(if(accepted = false, 1, 0)) as no_votes,
    sum(if(accepted = true, stakedAmount / 1e18, 0)) as yes_nxm_votes,
    sum(if(accepted = false, stakedAmount / 1e18, 0)) as no_nxm_votes
  from nexusmutual_ethereum.Assessment_evt_VoteCast
  group by 1
),

-- v2 assessment: Claims Committee expert-led voting (claim_id >= 29)
-- assessment start/end times (72 hour voting period)
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
vote_count_v2 as (
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

-- completed claims (old stake-weighted system, assessment_id <= 28)
completed_claims_old as (
  select
    c.submit_time,
    c.submit_date,
    c.cover_id,
    coalesce(c.assessment_id, c.claim_id) as assessment_id,
    c.product_id,
    c.cover_asset,
    c.requested_amount,
    coalesce(vc.yes_votes, 0) as yes_votes,
    coalesce(vc.no_votes, 0) as no_votes,
    coalesce(vc.yes_nxm_votes, 0) as yes_nxm_votes,
    coalesce(vc.no_nxm_votes, 0) as no_nxm_votes,
    coalesce(a.assessor_rewards, 0) as assessor_rewards,
    vc.last_vote_time,
    1 as assessment_version
  from claims c
    left join vote_count_v1 vc on c.assessment_id = vc.assessment_id
    left join assessments a on c.assessment_id = a.assessment_id
  where c.claim_id < 29 -- old assessment system
    and date_add('day', 3, coalesce(vc.first_vote_time, c.submit_time)) <= now()
    and (vc.last_vote_time is null
      or date_add('day', 1, vc.last_vote_time) <= now())
),

-- completed claims (Claims Committee system, claim_id >= 29)
-- voting ends after 72 hours + 24 hour cool-down period
completed_claims_new as (
  select
    c.submit_time,
    c.submit_date,
    c.cover_id,
    c.claim_id as assessment_id,
    c.product_id,
    c.cover_asset,
    c.requested_amount,
    coalesce(vc.yes_votes, 0) as yes_votes,
    coalesce(vc.no_votes, 0) as no_votes,
    cast(0 as double) as yes_nxm_votes, -- no stake weighting in v2
    cast(0 as double) as no_nxm_votes,
    cast(0 as double) as assessor_rewards, -- no NXM rewards for Claims Committee
    vc.last_vote_time,
    2 as assessment_version
  from claims c
    inner join assessment_timing at on c.claim_id = at.claim_id
    left join vote_count_v2 vc on c.claim_id = vc.claim_id
  where c.claim_id >= 29 -- new Claims Committee system
    and date_add('day', 1, at.voting_end_time) <= now() -- voting end + 24h cool-down
),

-- combine old and new completed claims
completed_claims as (
  select * from completed_claims_old
  union all
  select * from completed_claims_new
),

prices as (
  select
    date_trunc('day', minute) as block_date,
    symbol,
    avg(price) as usd_price
  from prices.usd
  where minute > timestamp '2019-05-01'
    and ((symbol = 'ETH' and blockchain is null and contract_address is null)
      or (symbol = 'DAI' and blockchain = 'ethereum' and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f)
      or (symbol = 'USDC' and blockchain = 'ethereum' and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48)
      or (symbol = 'cbBTC' and blockchain = 'ethereum' and contract_address = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf))
  group by 1, 2
)

select
  cc.assessment_id,
  cc.cover_id,
  case
    -- new Claims Committee: 2 of 3 votes needed for approval (simple majority)
    when cc.assessment_version = 2 and cc.yes_votes >= 2 then 'APPROVED ✅'
    when cc.assessment_version = 2 and cc.no_votes >= 2 then 'DENIED ❌'
    when cc.assessment_version = 2 then 'PENDING ⏳' -- shouldn't happen for completed claims
    -- old stake-weighted voting
    when cc.yes_nxm_votes > cc.no_nxm_votes and cc.yes_nxm_votes > 0 then 'APPROVED ✅'
    else 'DENIED ❌'
  end as verdict,
  if(cc.assessment_version = 2, concat(
    '<a href="https://app.nexusmutual.io/claims/details/',
    cast(cc.assessment_id as varchar),
    '" target="_blank">',
    'link',
    '</a>'
  )) as url_link,
  c.product_name,
  c.product_type,
  c.staking_pool as syndicate,
  c.cover_asset,
  c.sum_assured as cover_amount,
  c.sum_assured * p.usd_price as dollar_cover_amount,
  cc.requested_amount as claim_amount,
  cc.requested_amount * p.usd_price as dollar_claim_amount,
  c.cover_start_time,
  c.cover_end_time,
  cc.submit_time as claim_submit_time,
  cc.yes_votes,
  cc.no_votes,
  cc.yes_nxm_votes,
  cc.no_nxm_votes,
  case
    when cc.assessment_version = 2 then 0 -- no NXM rewards for Claims Committee
    when cc.yes_nxm_votes > cc.no_nxm_votes and cc.yes_nxm_votes > 0 then cc.assessor_rewards / cc.yes_votes
    when cc.no_nxm_votes > 0 then cc.assessor_rewards / cc.no_votes
    else 0
  end as assessor_rewards_per_vote,
  cc.assessor_rewards,
  cc.last_vote_time as last_vote
from covers c
  inner join completed_claims cc on c.cover_id = cc.cover_id and coalesce(c.product_id, cc.product_id) = cc.product_id
  inner join prices p on cc.submit_date = p.block_date and cc.cover_asset = p.symbol
order by cc.submit_time desc
