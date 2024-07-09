with

covers as (
  select
    1 as version,
    cover_id,
    cover_asset,
    sum_assured
  --from query_3788367 -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1
  union all
  select distinct
    2 as version,
    cover_id,
    cover_asset,
    sum_assured
  --from query_3788370 -- covers v2 base (fallback) query
  from nexusmutual_ethereum.covers_v2
  --where is_migrated = false
),

claims as (
  select
    1 as version,
    claim_id,
    cover_id,
    submit_date as claim_date,
    partial_claim_amount as claim_amount
  from query_3894606 -- claims v1 base (fallback) query
  where claim_status = 14
    or claim_id = 102
  union all
  select
    2 as version,
    claim_id,
    cover_id,
    submit_date as claim_date,
    requested_amount as claim_amount
  from query_3894982 -- claims v2 base (fallback) query
),

claims_paid as (
  select
    cl.version,
    cl.claim_id,
    cl.claim_date,
    c.cover_asset,
    coalesce(cl.claim_amount, c.sum_assured) as claim_amount,
    if(c.cover_asset = 'ETH', coalesce(cl.claim_amount, c.sum_assured), 0) as eth_claim_amount,
    if(c.cover_asset = 'DAI', coalesce(cl.claim_amount, c.sum_assured), 0) as dai_claim_amount,
    if(c.cover_asset = 'USDC', coalesce(cl.claim_amount, c.sum_assured), 0) as usdc_claim_amount
  from covers c
    inner join claims cl on c.cover_id = cl.cover_id and c.version = cl.version
    left join (
        select
          claimId as claim_id,
          row_number() over (partition by call_block_time, call_tx_hash, claimId order by call_trace_address desc) as rn
        from nexusmutual_ethereum.IndividualClaims_call_redeemClaimPayout
        where call_success
      ) cp on cl.claim_id = cp.claim_id and cl.version = 2 and cp.rn = 1
  where cl.version = 1
    or (cl.version = 2 and cp.claim_id is not null)
),

prices as (
  select
    date_trunc('day', minute) as block_date,
    symbol,
    avg(price) as avg_price_usd
  from prices.usd
  where minute > timestamp '2019-05-01'
    and ((symbol = 'ETH' and blockchain is null and contract_address is null)
      or (symbol = 'DAI' and blockchain = 'ethereum' and contract_address = 0x6b175474e89094c44da98b954eedeac495271d0f)
      or (symbol = 'USDC' and blockchain = 'ethereum' and contract_address = 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48))
  group by 1, 2
),

claims_paid_enriched as (
  select
    cp.version,
    cp.claim_id,
    cp.claim_date,
    --ETH
    cp.eth_claim_amount as eth_eth_claim_amount,
    cp.eth_claim_amount * p.avg_price_usd as eth_usd_claim_amount,
    --DAI
    cp.dai_claim_amount * p.avg_price_usd / p.avg_price_usd as dai_eth_claim_amount,
    cp.dai_claim_amount * p.avg_price_usd as dai_usd_claim_amount,
    --USDC
    cp.usdc_claim_amount * p.avg_price_usd / p.avg_price_usd as usdc_eth_claim_amount,
    cp.usdc_claim_amount * p.avg_price_usd as usdc_usd_claim_amount
  from claims_paid cp
    inner join prices p on cp.claim_date = p.block_date and cp.cover_asset = p.symbol
)

select
  claim_date,
  sum(if('{{display_currency}}' = 'USD', eth_usd_claim_amount, eth_eth_claim_amount)) over (order by claim_date) as eth_claim_total,
  sum(if('{{display_currency}}' = 'USD', dai_usd_claim_amount, dai_eth_claim_amount)) over (order by claim_date) as dai_claim_total,
  sum(if('{{display_currency}}' = 'USD', usdc_usd_claim_amount, usdc_eth_claim_amount)) over (order by claim_date) as usdc_claim_total,
  sum(if(
    '{{display_currency}}' = 'USD',
    eth_usd_claim_amount + dai_usd_claim_amount + usdc_usd_claim_amount,
    eth_eth_claim_amount + dai_eth_claim_amount + usdc_eth_claim_amount
  )) over (order by claim_date) as claim_total
from claims_paid_enriched
where claim_date >= timestamp '{{Start Date}}'
  and claim_date < timestamp '{{End Date}}'
order by 1 desc
