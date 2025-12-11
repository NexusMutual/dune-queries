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

vote_count as (
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

completed_claims as (
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
    vc.last_vote_time
  from claims c
    left join vote_count vc on c.assessment_id = vc.assessment_id
    left join assessments a on c.assessment_id = a.assessment_id
  where date_add('day', 3, coalesce(vc.first_vote_time, c.submit_time)) <= now()
    and (vc.last_vote_time is null
      or date_add('day', 1, vc.last_vote_time) <= now())
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
    when (cc.yes_nxm_votes > cc.no_nxm_votes and cc.yes_nxm_votes > 0) or cc.assessment_id >= 29 then 'APPROVED ✅'
    else 'DENIED ❌'
  end as verdict,
  if(cc.assessment_id >= 29, concat(
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
