with

covers as (
  select
    cover_id,
    cover_start_time,
    cover_end_time,
    cover_start_date,
    cover_end_date,
    product_contract,
    syndicate,
    product_name,
    product_type,
    cover_asset,
    sum_assured
  --from query_3788367 -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1
),

claims as (
  select
    claim_id,
    cover_id,
    submit_time,
    submit_date,
    partial_claim_amount,
    claim_status
  --from query_3894606 -- claims v1 base (fallback) query
  from nexusmutual_ethereum.claims_v1
),

ca_votes as (
  select
    _claimId as claim_id,
    sum(if(_vote = 1, 1, 0)) as ca_vote_yes,
    sum(if(_vote = -1, 1, 0)) as ca_vote_no,
    sum(if(_vote = 1, _tokens / 1e18, 0)) as ca_nxm_vote_yes,
    sum(if(_vote = -1, _tokens / 1e18, 0)) as ca_nxm_vote_no,
    sum(_tokens) / 1e18 as ca_nxm_vote_total
  from nexusmutual_ethereum.ClaimsData_call_setClaimTokensCA
  where call_success
  group by 1
),

mv_votes as (
  select
    _claimId as claim_id,
    sum(if(_vote = 1, 1, 0)) as mv_vote_yes,
    sum(if(_vote = -1, 1, 0)) as mv_vote_no,
    sum(if(_vote = 1, _tokens / 1e18, 0)) as mv_nxm_vote_yes,
    sum(if(_vote = -1, _tokens / 1e18, 0)) as mv_nxm_vote_no,
    sum(_tokens) / 1e18 as mv_nxm_vote_total
  from nexusmutual_ethereum.ClaimsData_call_setClaimTokensMV
  where call_success
  group by 1
),

votes as (
  select
    coalesce(ca_votes.claim_id, mv_votes.claim_id) as claim_id,
    ca_vote_yes,
    ca_vote_no,
    ca_nxm_vote_yes,
    ca_nxm_vote_no,
    ca_nxm_vote_total,
    mv_vote_yes,
    mv_vote_no,
    mv_nxm_vote_yes,
    mv_nxm_vote_no,
    mv_nxm_vote_total
  from ca_votes
    full join mv_votes on ca_votes.claim_id = mv_votes.claim_id
),

quorum as (
  select
    t.claimId as claim_id,
    sum(t.output_tokens) filter (where t.member = 0) as ca_tokens,
    sum(p.output_tokenPrice) filter (where t.member = 0) as ca_quorum_price,
    sum(t.output_tokens) filter (where t.member = 1) as mv_tokens,
    sum(p.output_tokenPrice) filter (where t.member = 1) as mv_quorum_price
  from nexusmutual_ethereum.Pool_call_getTokenPrice p
    inner join nexusmutual_ethereum.Claims_call_getCATokens t on p.call_block_time = t.call_block_time
      and p.call_tx_hash = t.call_tx_hash
      and contains_sequence(p.call_trace_address, t.call_trace_address)
  where p.call_success
    and t.call_success
  group by 1
  union all
  select
    t.claimId as claim_id,
    sum(t.output_tokens) filter (where t.member = 0) as ca_tokens,
    sum(p.output_tokenPrice) filter (where t.member = 0) as ca_quorum_price,
    sum(t.output_tokens) filter (where t.member = 1) as mv_tokens,
    sum(p.output_tokenPrice) filter (where t.member = 1) as mv_quorum_price
  from nexusmutual_ethereum.MCR_call_calculateTokenPrice p
    inner join nexusmutual_ethereum.Claims_call_getCATokens t on p.call_block_time = t.call_block_time
      and p.call_tx_hash = t.call_tx_hash
      and contains_sequence(p.call_trace_address, t.call_trace_address)
  where p.call_success
    and t.call_success
  group by 1
),

votes_quorum as (
  select
    coalesce(votes.claim_id, quorum.claim_id) as claim_id,
    ca_vote_yes,
    ca_vote_no,
    ca_nxm_vote_yes,
    ca_nxm_vote_no,
    ca_nxm_vote_total,
    ca_quorum_price,
    mv_vote_yes,
    mv_vote_no,
    mv_nxm_vote_yes,
    mv_nxm_vote_no,
    mv_nxm_vote_total,
    mv_quorum_price
  from quorum
    full join votes on votes.claim_id = quorum.claim_id
),

assessor_rewards as (
  select
    claimid as claim_id,
    tokens / 1e18 as nxm_assessor_rewards
  from nexusmutual_ethereum.ClaimsData_call_setClaimRewardDetail
  where call_success
),

prices as (
  select
    date_trunc('day', minute) as block_date,
    symbol,
    avg(price) as usd_price
  from prices.usd
  where minute > timestamp '2019-05-01'
    and ((symbol = 'ETH' and blockchain is null)
      or (symbol = 'DAI' and blockchain = 'ethereum' and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f))
  group by 1, 2
),

claim_status_details as (
  select
    c.cover_id,
    cs.claim_id,
    cs.submit_time,
    c.cover_start_time,
    c.cover_end_time,
    ar.nxm_assessor_rewards as assessor_rewards,
    c.cover_asset,
    c.syndicate,
    c.product_name,
    c.product_type,
    cs.claim_status,
    coalesce(cs.partial_claim_amount, c.sum_assured) as claim_amount,
    coalesce(cs.partial_claim_amount, c.sum_assured) * p.usd_price as dollar_claim_amount,
    c.sum_assured,
    c.sum_assured * p.usd_price as dollar_sum_assured
  from claims cs
    inner join covers c on cs.cover_id = c.cover_id
    inner join prices p on cs.submit_date = p.block_date and c.cover_asset = p.symbol
    left join assessor_rewards ar on cs.claim_id = ar.claim_id
  where cs.claim_status in (6, 9, 11, 12, 13, 14) -- only get final status's
)

select
  csd.claim_id,
  csd.cover_id,
  case
    -- clim id 108 is a snapshot vote that was accepted
    when csd.claim_status in (12, 13, 14) or csd.claim_id = 102 then 'ACCEPTED ✅'
    else 'DENIED ❌'
  end as verdict,
  concat(
    '<a href="https://app.nexusmutual.io/claim-assessment/view-claim?claimId=',
    cast(csd.claim_id as varchar),
    '" target="_blank">',
    'link',
    '</a>'
  ) as url_link,
  csd.product_name,
  csd.product_type,
  csd.syndicate as staking_pool,
  csd.cover_asset,
  csd.sum_assured,
  csd.dollar_sum_assured,
  csd.claim_amount,
  csd.dollar_claim_amount,
  csd.cover_start_time,
  csd.cover_end_time,
  csd.submit_time,
  vq.ca_vote_yes,
  vq.ca_vote_no,
  vq.ca_nxm_vote_yes,
  vq.ca_nxm_vote_no,
  vq.ca_vote_yes + vq.ca_vote_no as ca_total_votes,
  vq.ca_nxm_vote_total,
  csd.assessor_rewards / (vq.ca_vote_yes + vq.ca_vote_no) as nxm_assessor_rewards_per_vote,
  csd.sum_assured * 5 * 1e18 / vq.ca_quorum_price as ca_nxm_quorum,
  vq.mv_vote_yes,
  vq.mv_vote_no,
  vq.mv_vote_yes + vq.mv_vote_no as mv_total_votes,
  vq.mv_nxm_vote_yes,
  vq.mv_nxm_vote_no,
  vq.mv_nxm_vote_total,
  csd.sum_assured * 5 * 1e18 / vq.mv_quorum_price as mv_nxm_quorum,
  csd.assessor_rewards,
  case
    when vq.ca_vote_yes > vq.ca_vote_no then csd.assessor_rewards / vq.ca_vote_yes
    when vq.ca_vote_yes < vq.ca_vote_no then csd.assessor_rewards / vq.ca_vote_no
    else 0
  end as nxm_ca_assessor_rewards_per_vote,
  case
    when vq.mv_vote_yes > vq.mv_vote_no then csd.assessor_rewards / vq.mv_vote_yes
    when vq.mv_vote_yes < vq.mv_vote_no then csd.assessor_rewards / vq.mv_vote_no
    else 0
  end as nxm_mv_assessor_rewards_per_vote
from claim_status_details csd
  left join votes_quorum vq on csd.claim_id = vq.claim_id
order by csd.claim_id desc
