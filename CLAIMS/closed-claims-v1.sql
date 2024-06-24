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
  from query_3788367 -- covers v1 base (fallback) query
),

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
),

claims_status as (
  select claim_id, cover_id, submit_time, submit_date, partial_claim_amount, claim_status
  from (
    select
      c.claim_id,
      c.cover_id,
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
),

assessor_rewards as (
  select
    claimid as claim_id,
    tokens / 1e18 as nxm_assessor_rewards
  from nexusmutual_ethereum.ClaimsData_call_setClaimRewardDetail
  where call_success
),

-- CA & MV votes
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
  from nexusmutual_ethereum.ClaimsData_call_setClaimTokensCA
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

-- quorum caculation
ca_tokens as (
  select
    call_block_time,
    claimId as claim_id,
    output_tokens as tokens,
    call_tx_hash,
    call_trace_address
  from nexusmutual_ethereum.Claims_call_getCATokens
  where call_success
    and member = 0
),

mv_tokens as (
  select
    call_block_time,
    claimId as claim_id,
    output_tokens as tokens,
    call_tx_hash,
    call_trace_address
  from nexusmutual_ethereum.Claims_call_getCATokens
  where call_success
    and member = 1
),

ca_quorum as (
  select
    t.claim_id,
    t.tokens,
    p.output_tokenPrice as ca_quorum_price,
    t.call_block_time as ca_ts
  from nexusmutual_ethereum.Pool_call_getTokenPrice p
    inner join ca_tokens t on p.call_block_time = t.call_block_time
      and p.call_tx_hash = t.call_tx_hash
      and contains_sequence(p.call_trace_address, t.call_trace_address)
  where p.call_success
  union all
  select
    t.claim_id,
    t.tokens,
    p.output_tokenPrice as ca_quorum_price,
    t.call_block_time as ca_ts
  from nexusmutual_ethereum.MCR_call_calculateTokenPrice p
    inner join ca_tokens t on p.call_block_time = t.call_block_time
      and p.call_tx_hash = t.call_tx_hash
      and contains_sequence(p.call_trace_address, t.call_trace_address)
  where p.call_success
),

mv_quorum as (
  select
    t.claim_id,
    t.tokens,
    p.output_tokenPrice as mv_quorum_price,
    t.call_block_time as mv_ts
  from nexusmutual_ethereum.Pool_call_getTokenPrice p
    inner join mv_tokens t on p.call_block_time = t.call_block_time
      and p.call_tx_hash = t.call_tx_hash
      and contains_sequence(p.call_trace_address, t.call_trace_address)
  where p.call_success
  union all
  select
    t.claim_id,
    t.tokens,
    p.output_tokenPrice as mv_quorum_price,
    t.call_block_time as mv_ts
  from nexusmutual_ethereum.MCR_call_calculateTokenPrice p
    inner join mv_tokens t on p.call_block_time = t.call_block_time
      and p.call_tx_hash = t.call_tx_hash
      and contains_sequence(p.call_trace_address, t.call_trace_address)
  where p.call_success
),

quorum as (
  select
    coalesce(ca_quorum.claim_id, mv_quorum.claim_id) as claim_id,
    ca_quorum_price,
    ca_ts,
    mv_quorum_price,
    mv_ts
  from ca_quorum
    full join mv_quorum on ca_quorum.claim_id = mv_quorum.claim_id
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

prices as (
  select
    date_trunc('day', minute) as block_date,
    symbol,
    avg(price) as usd_price
  from prices.usd
  where minute > timestamp '2019-05-01'
    and ((symbol = 'ETH' and blockchain is null)
      or (symbol = 'DAI' and blockchain = 'ethereum'))
  group by 1, 2
),

claims_status_details as (
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
    coalesce(cs.partial_claim_amount, c.sum_assured) * p_claim.usd_price as dollar_claim_amount,
    c.sum_assured,
    c.sum_assured * p_cover.usd_price as dollar_sum_assured
  from claims_status cs
    inner join covers c on cs.cover_id = c.cover_id
    inner join prices p_claim on cs.submit_date = p_claim.block_date and c.cover_asset = p_claim.symbol
    inner join prices p_cover on c.cover_start_date = p_cover.block_date and c.cover_asset = p_cover.symbol
    left join assessor_rewards ar on cs.claim_id = ar.claim_id
  where cs.claim_status in (6, 9, 11, 12, 13, 14) -- only get final status's
),

claims_status_details_votes as (
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
      when ca_vote_yes > ca_vote_no then assessor_rewards / ca_vote_yes
      when ca_vote_yes < ca_vote_no then assessor_rewards / ca_vote_no
      else 0
    end as nxm_ca_assessor_rewards_per_vote,
    case
      when mv_vote_yes > mv_vote_no then assessor_rewards / mv_vote_yes
      when mv_vote_yes < mv_vote_no then assessor_rewards / mv_vote_no
      else 0
    end as nxm_mv_assessor_rewards_per_vote
  from claims_status_details csd
    left join votes_quorum vq on csd.claim_id = vq.claim_id
)

select *
from claims_status_details_votes
order by claim_id desc
