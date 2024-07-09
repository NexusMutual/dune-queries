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
  where is_migrated = false
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
    claim_date,
    c.cover_asset,
    coalesce(cl.claim_amount, c.sum_assured) as claim_amount
  from covers c
    inner join claims cl on c.cover_id = cl.cover_id and c.version = cl.version
    left join nexusmutual_ethereum.IndividualClaims_call_redeemClaimPayout cp on cl.claim_id = cp.claimId
      and cl.version = 2
      and cp.call_success
  where cl.version = 1
    or (cl.version = 2 and cp.claimId is not null)
)

select * from claims_paid


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

total_claims AS (
  SELECT
    claim_date,
    SUM(
      CASE
        WHEN cover_asset = 'DAI' AND '{{display_currency}}' = 'USD' then claim_amount * dai_price_dollar
        WHEN cover_asset = 'DAI' AND '{{display_currency}}' = 'ETH' then claim_amount * dai_price_dollar / eth_price_dollar
        ELSE 0
      END
    ) OVER (ORDER BY claim_date) AS running_dai_claimed,
    SUM(
      CASE
        WHEN cover_asset = 'ETH' AND '{{display_currency}}' = 'USD' then claim_amount * eth_price_dollar
        WHEN cover_asset = 'ETH' AND '{{display_currency}}' = 'ETH' then claim_amount
        ELSE 0
      END
    ) OVER (ORDER BY claim_date) AS running_eth_claimed,
    SUM(
      CASE
        WHEN cover_asset = 'ETH' AND '{{display_currency}}' = 'USD' then claim_amount * eth_price_dollar
        WHEN cover_asset = 'ETH' AND '{{display_currency}}' = 'ETH' then claim_amount
        WHEN cover_asset = 'DAI' AND '{{display_currency}}' = 'USD' then claim_amount * dai_price_dollar
        WHEN cover_asset = 'DAI' AND '{{display_currency}}' = 'ETH' then claim_amount * dai_price_dollar / eth_price_dollar
        ELSE 0
      END
    ) OVER (ORDER BY claim_date) AS running_total_claimed
  from claims_paid cp
    inner join prices p on cp.claim_date = p.block_date and cp.cover_asset = p.symbol
)

select
  *
from total_claims
where claim_date >= timestamp '{{Start Date}}'
  and claim_date <= timestamp '{{End Date}}'
order by claim_date desc
