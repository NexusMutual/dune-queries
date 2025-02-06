with

daily_avg_prices as (
  select
    block_date,
    avg_nxm_eth_price,
    avg_nxm_usd_price
  --from query_3789851 -- prices base (fallback) query
  from nexusmutual_ethereum.capital_pool_prices
),

v1_vote_count as (
  select
    claimId as claim_id,
    case when sum(tokens * 1e-18 * verdict) > 0 then 1 else -1 end as result,
    sum(case when verdict = 1 then 1 else 0 end) as yes_votes,
    sum(case when verdict = -1 then 1 else 0 end) as no_votes,
    sum(case when verdict = 1 then tokens * 1e-18 else 0 end) as yes_nxm_votes,
    sum(case when verdict = -1 then tokens * 1e-18 else 0 end) as no_nxm_votes
  from nexusmutual_ethereum.ClaimsData_evt_VoteCast
  group by 1
),

v1_assessor_rewards as (
  select
    vc.claim_id,
    t.call_block_time as block_time,
    t.tokens * 1e-18 as total_nxm_rewards,
    vc.yes_votes,
    vc.no_votes,
    vc.yes_nxm_votes,
    vc.no_nxm_votes,
    vc.result as verdict,
    case when vc.result = 1 then vc.yes_votes else vc.no_votes end as winning_votes_count
  from nexusmutual_ethereum.ClaimsData_call_setClaimRewardDetail t
    inner join v1_vote_count vc on t.claimId = vc.claim_id
  where t.call_success
),

v2_vote_count as (
  select
    assessmentId as claim_id,
    sum(case when accepted = true then 1 else 0 end) as yes_votes,
    sum(case when accepted = true then stakedAmount * 1e-18 else 0 end) as yes_nxm_votes,
    sum(case when accepted = false then 1 else 0 end) as no_votes,
    sum(case when accepted = false then stakedAmount * 1e-18 else 0 end) as no_nxm_votes
  from nexusmutual_ethereum.Assessment_evt_VoteCast
  group by 1
),

v2_assessor_rewards as (
  select distinct
    vc.claim_id,
    t.call_block_time as block_time,
    t.output_totalRewardInNXM * 1e-18 as total_nxm_rewards,
    vc.yes_votes,
    vc.no_votes,
    vc.yes_nxm_votes,
    vc.no_nxm_votes,
    case when vc.yes_nxm_votes > vc.no_nxm_votes then 1 else -1 end as verdict,
    case when vc.yes_nxm_votes > vc.no_nxm_votes then vc.yes_votes else vc.no_votes end as winning_votes_count
  from nexusmutual_ethereum.Assessment_call_assessments t
    inner join v2_vote_count vc on t._0 = vc.claim_id
  where t.call_success
),

assessor_rewards as (
  select
    block_time,
    'v1' as version,
    claim_id,
    total_nxm_rewards,
    winning_votes_count,
    total_nxm_rewards / winning_votes_count as average_reward_per_claim
  from v1_assessor_rewards
  union all
  select
    block_time,
    'v2' as version,
    claim_id,
    total_nxm_rewards,
    winning_votes_count,
    total_nxm_rewards / winning_votes_count as average_reward_per_claim
  from v2_assessor_rewards
),

running_assessor_rewards as (
  select
    ar.block_time,
    ar.version,
    ar.claim_id,
    ar.winning_votes_count,
    ar.total_nxm_rewards,
    ar.total_nxm_rewards * p.avg_nxm_eth_price as total_nxm_eth_rewards,
    ar.total_nxm_rewards * p.avg_nxm_usd_price as total_nxm_usd_rewards,
    sum(ar.winning_votes_count) over (order by ar.block_time) as total_winning_votes,
    sum(ar.total_nxm_rewards) over (order by ar.block_time) as total_assessor_nxm_rewards,
    sum(ar.total_nxm_rewards * p.avg_nxm_eth_price) over (order by ar.block_time) as total_assessor_nxm_eth_rewards,
    sum(ar.total_nxm_rewards * p.avg_nxm_usd_price) over (order by ar.block_time) as total_assessor_nxm_usd_rewards
  from assessor_rewards ar
    inner join daily_avg_prices p on date_trunc('day', ar.block_time) = p.block_date
)

select
  block_time,
  version,
  claim_id,
  winning_votes_count,
  if('{{display_currency}}' = 'USD', total_nxm_usd_rewards, total_nxm_eth_rewards) as total_rewards,
  total_winning_votes,
  if('{{display_currency}}' = 'USD', total_assessor_nxm_usd_rewards, total_assessor_nxm_eth_rewards) as total_assessor_rewards,
  if('{{display_currency}}' = 'USD', total_assessor_nxm_usd_rewards, total_assessor_nxm_eth_rewards) / total_winning_votes as avg_reward_per_claim
from running_assessor_rewards
order by 1 desc
