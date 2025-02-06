with

votes_count as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'v1' as version,
    claimId as claim_id,
    if(verdict = 1, true, false) as result,
    tokens / 1e18 as nxm_vote,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.ClaimsData_evt_VoteCast
  union all
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    'v2' as version,
    assessmentId as claim_id,
    accepted as result,
    stakedAmount / 1e18 as nxm_vote,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.Assessment_evt_VoteCast
),

votes_with_gas_usage as (
  select
    vc.version,
    avg(t.tx_fee) as tx_fee_eth,
    avg(t.tx_fee_usd) as tx_fee_usd
  from gas_ethereum.fees t
    inner join votes_count vc
      on t.block_number = vc.block_number
      and t.block_time = vc.block_time
      and t.tx_hash = vc.tx_hash
  where t.block_time >= timestamp '2019-05-01'
  group by 1
)

select
  if('{{display_currency}}' = 'USD', v1.tx_fee_usd, v1.tx_fee_eth) as avg_tx_fee_v1,
  if('{{display_currency}}' = 'USD', v2.tx_fee_usd, v2.tx_fee_eth) as avg_tx_fee_v2
from votes_with_gas_usage v1
  cross join votes_with_gas_usage v2
where v1.version = 'v1'
  and v2.version = 'v2'
