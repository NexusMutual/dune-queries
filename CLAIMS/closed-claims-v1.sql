with

prices as (
  SELECT DISTINCT
    DATE_TRUNC('day', minute) as minute,
    symbol,
    AVG(price) OVER (
      PARTITION BY
        symbol,
        DATE_TRUNC('day', minute)
    ) as price_dollar
  FROM prices.usd
  WHERE minute > CAST('2019-05-01' as TIMESTAMP)
    and ((symbol = 'ETH' and blockchain is null)
      or (symbol = 'DAI' and blockchain = 'ethereum'))
),

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
  from_unixtime(cr.dateSubmit) as date_submit,
  if(cr.claimId = 102, cast(10.43 as double), cast(cp.requestedPayoutAmount as double)) as partial_claim_amount
from nexusmutual_ethereum.ClaimsData_evt_ClaimRaise cr
  left join nexusmutual_ethereum.Claims_call_submitPartialClaim cp on cr.coverId = cp.coverId
    and cr.evt_tx_hash = cp.call_tx_hash
    and cp.requestedPayoutAmount > 0
    and cp.call_success
),

claims_status as (
  select claim_id, cover_id, date_submit, partial_claim_amount, claim_status
  from (
    select
      c.claim_id,
      c.cover_id,
      c.date_submit,
      c.partial_claim_amount,
      cs._stat as claim_status,
      row_number() over (partition by c.claim_id order by cs._stat desc) as rn
    from nexusmutual_ethereum.ClaimsData_call_setClaimStatus cs
      inner join claims c ON cs._claimId = c.claim_id
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

-- CA & MV vote calculation
ca_votes as (
  select distinct
    _claimId as claim_id,
    sum(case when _vote = 1 then 1 else 0 end) over (partition by _claimId) as ca_vote_yes,
    sum(case when _vote = -1 then 1 else 0 end) over (partition by _claimId) as ca_vote_no,
    sum(case when _vote = 1 then _tokens / 1e18 else 0 end) over (partition by _claimId) as ca_nxm_vote_yes,
    sum(case when _vote = -1 then _tokens / 1e18 else 0 end) over (partition by _claimId) as ca_nxm_vote_no,
    sum(_tokens) over (partition by _claimId) / 1e18 as ca_total_tokens
  from nexusmutual_ethereum.ClaimsData_call_setClaimTokensCA
  where call_success
),

mv_votes as (
  select distinct
    _claimId as claim_id,
    sum(case when _vote = 1 then 1 else 0 end) over (partition by _claimId) as mv_vote_yes,
    sum(case when _vote = -1 then 1 else 0 end) over (partition by _claimId) as mv_vote_no,
    sum(case when _vote = 1 then _tokens / 1e18 else 0 end) over (partition by _claimId) as mv_nxm_vote_yes,
    sum(case when _vote = -1 then _tokens / 1e18 else 0 end) over (partition by _claimId) as mv_nxm_vote_no,
    sum(_tokens) over (partition by _claimId) / 1e18 as mv_total_tokens
  from nexusmutual_ethereum.ClaimsData_call_setClaimTokensMV
  where call_success
),

  votes as (
    SELECT
      COALESCE(ca_votes.claim_id, mv_votes.claim_id) as claim_id,
      ca_vote_yes,
      ca_vote_no,
      ca_nxm_vote_yes,
      ca_nxm_vote_no,
      ca_total_tokens,
      mv_vote_yes,
      mv_vote_no,
      mv_nxm_vote_yes,
      mv_nxm_vote_no,
      mv_total_tokens
    FROM
      ca_votes
      FULL JOIN mv_votes ON ca_votes.claim_id = mv_votes.claim_id
  ),
  CA_token /* QUORUM CACULATION */ as (
    SELECT
      claimId as claim_id,
      CASE
        WHEN CAST(member as INT) = 0 THEN output_tokens
      END as CA_tokens,
      call_tx_hash as ca_tx_hash,
      call_trace_address as ca_call_address
    FROM
      nexusmutual_ethereum.Claims_call_getCATokens as t
    WHERE
      t.call_success = TRUE
      AND CAST(member as INT) = 0
  ),
  MV_token /* join CA_tokens and MV_tokens to get all token quests */ as (
    SELECT
      claimId as claim_id,
      CASE
        WHEN CAST(member as INT) = 1 THEN output_tokens
      END as MV_tokens,
      call_tx_hash as mv_tx_hash,
      call_trace_address as mv_call_address
    FROM
      nexusmutual_ethereum.Claims_call_getCATokens as t
    WHERE
      t.call_success = TRUE
      AND CAST(member as INT) = 1
  ),
  mv_quorum as (
    SELECT
      claim_id,
      output_tokenPrice as mv_quorum_price,
      t.call_block_time as mv_ts
    FROM
      nexusmutual_ethereum.Pool_call_getTokenPrice as t
      RIGHT JOIN MV_token ON MV_token.mv_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, MV_token.mv_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
    UNION
    SELECT
      claim_id,
      output_tokenPrice as mv_quorum_price,
      t.call_block_time as mv_ts
    FROM
      nexusmutual_ethereum.MCR_call_calculateTokenPrice as t
      RIGHT JOIN MV_token ON MV_token.mv_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, MV_token.mv_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
  ),
  ca_quorum as (
    SELECT
      claim_id,
      output_tokenPrice as ca_quorum_price,
      t.call_block_time as ca_ts
    FROM
      nexusmutual_ethereum.Pool_call_getTokenPrice as t
      RIGHT JOIN CA_token ON CA_token.ca_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, CA_token.ca_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
    UNION
    SELECT
      claim_id,
      output_tokenPrice as ca_quorum_price,
      t.call_block_time as ca_ts
    FROM
      nexusmutual_ethereum.MCR_call_calculateTokenPrice as t
      RIGHT JOIN CA_token ON CA_token.ca_tx_hash = t.call_tx_hash
      and contains_sequence(t.call_trace_address, CA_token.ca_call_address)
    WHERE
      t.call_success = TRUE
      AND claim_id IS NOT NULL
  ),
  quorum as (
    SELECT DISTINCT
      COALESCE(ca_quorum.claim_id, mv_quorum.claim_id) as claim_id,
      ca_quorum_price,
      ca_ts,
      mv_quorum_price,
      mv_ts
    FROM
      ca_quorum
      FULL JOIN mv_quorum ON ca_quorum.claim_id = mv_quorum.claim_id
  ),
  votes_quorum as (
    SELECT DISTINCT
      COALESCE(votes.claim_id, quorum.claim_id) as claim_id,
      ca_vote_yes,
      ca_vote_no,
      ca_nxm_vote_yes,
      ca_nxm_vote_no,
      ca_total_tokens,
      ca_quorum_price,
      mv_vote_yes,
      mv_vote_no,
      mv_nxm_vote_yes,
      mv_nxm_vote_no,
      mv_total_tokens,
      mv_quorum_price
    FROM
      quorum
      FULL JOIN votes ON votes.claim_id = quorum.claim_id
  ),
  claims_status_details as (
    SELECT
      s.cover_id,
      claim_id,
      date_submit,
      s.cover_start_time,
     s.cover_end_time,
      assessor_rewards.nxm_assessor_rewards as assessor_rewards,
      s.cover_asset,
      s.syndicate,
      s.product_name,
      s.product_type,
      max_status_no,
      COALESCE(
        CAST(partial_claim_amount as DOUBLE),
        CAST(sum_assured as DOUBLE)
      ) as claim_amount,
      claim_t.price_dollar * COALESCE(
        CAST(partial_claim_amount as DOUBLE),
        CAST(sum_assured as DOUBLE)
      ) as dollar_claim_amount,
      s.sum_assured,
      cover_t.price_dollar * s.sum_assured as dollar_sum_assured
     FROM
      claims_status as t
      LEFT JOIN cover_details as s ON s.cover_id = t.cover_id
      LEFT JOIN assessor_rewards ON assessor_rewards.claimId = t.claim_id
      INNER JOIN prices as claim_t ON claim_t.minute = DATE_TRUNC('day', t.date_submit)
      AND claim_t.symbol = s.cover_asset
      INNER JOIN prices as cover_t ON cover_t.minute = DATE_TRUNC('day', s.cover_start_time)
      AND cover_t.symbol = s.cover_asset
    WHERE
      max_status_no IS NOT NULL
      AND CAST(max_status_no as INT) IN (6, 9, 11, 12, 13, 14) -- only get final status's
  ),
  claims_status_details_votes as (
    SELECT
      CAST(claims_status_details.claim_id as BIGINT) as claim_id,
      claims_status_details.cover_id,
      case
      -- clim id 108 is a snapshot vote that was accepted
        when CAST(max_status_no as INT) IN (12, 13, 14)
        OR claims_status_details.claim_id = CAST(102 as UINT256) then 'ACCEPTED ✅'
        ELSE 'DENIED ❌'
      end as verdict,
      CONCAT(
        '<a href="https://app.nexusmutual.io/claim-assessment/view-claim?claimId=',
        CAST(claims_status_details.claim_id as VARCHAR),
        '" target="_blank">',
        'link',
        '</a>'
      ) as url_link,
      claims_status_details.product_name,
      claims_status_details.product_type,
      claims_status_details.syndicate as staking_pool,
      claims_status_details.cover_asset,
      sum_assured,
      dollar_sum_assured,
      claim_amount,
      dollar_claim_amount,
      claims_status_details.cover_start_time,
      claims_status_details.cover_end_time,
      claims_status_details.date_submit as date_submit,
      votes_quorum.ca_vote_yes,
      votes_quorum.ca_vote_no,
      votes_quorum.ca_nxm_vote_yes,
      votes_quorum.ca_nxm_vote_no,
      votes_quorum.ca_vote_yes + votes_quorum.ca_vote_no as ca_total_votes,
      votes_quorum.ca_total_tokens,
      claims_status_details.assessor_rewards / (
        votes_quorum.ca_vote_yes + votes_quorum.ca_vote_no
      ) as nxm_assessor_rewards_per_vote,
      claims_status_details.sum_assured * 5 * 1E18 / votes_quorum.ca_quorum_price as ca_nxm_quorum,
      votes_quorum.mv_vote_yes,
      votes_quorum.mv_vote_no,
      votes_quorum.mv_vote_yes + votes_quorum.mv_vote_no as mv_total_votes,
      votes_quorum.mv_nxm_vote_yes,
      votes_quorum.mv_nxm_vote_no,
      votes_quorum.mv_total_tokens,
      claims_status_details.sum_assured * 5 * 1E18 / votes_quorum.mv_quorum_price as mv_nxm_quorum,
      claims_status_details.assessor_rewards,
      case
        when ca_vote_yes > ca_vote_no THEN assessor_rewards / ca_vote_yes
        when ca_vote_yes < ca_vote_no THEN assessor_rewards / ca_vote_no
        ELSE 0
      end as nxm_ca_assessor_rewards_per_vote,
      case
        when mv_vote_yes > mv_vote_no THEN assessor_rewards / mv_vote_yes
        when mv_vote_yes < mv_vote_no THEN assessor_rewards / mv_vote_no
        ELSE 0
      end as nxm_mv_assessor_rewards_per_vote
     FROM
      claims_status_details
      LEFT JOIN votes_quorum ON votes_quorum.claim_id = claims_status_details.claim_id
  )
SELECT
  *
FROM
  claims_status_details_votes
ORDER BY
  claim_id DESC