with

covers_v1 as (
  select
    cover_id,
    cover_asset,
    sum_assured
  --from query_3788367 -- covers v1 base (fallback) query
  from nexusmutual_ethereum.covers_v1
),

claims_v1 as (
  select
    claim_id,
    cover_id,
    submit_time,
    submit_date,
    partial_claim_amount,
    claim_status
  from query_3894606 -- claims v1 base (fallback) query
  where claim_status = 14 -- ??
),

claims_paid_v1 as (
  select
    cl.claim_id,
    cl.submit_date as claim_date,
    c.cover_asset,
    coalesce(cl.partial_claim_amount, c.sum_assured) as claim_amount
  from covers_v1 c
    inner join claims_v1 cl on c.cover_id = cl.cover_id
),

covers_v2 as (
  select distinct
    cover_id,
    cover_asset,
    sum_assured
  from query_3788370 -- covers v2 base (fallback) query
  --from nexusmutual_ethereum.covers_v2 -- spell needs updating - add product_id
),

claims_v2 as (
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
),

claims_paid_v2 as (
  select
    cl.claim_id,
    cl.submit_date as claim_date,
    c.cover_asset,
    cl.requested_amount as claim_amount
  from covers_v2 c
    inner join claims_v2 cl on c.cover_id = cl.cover_id
    inner join nexusmutual_ethereum.IndividualClaims_call_redeemClaimPayout cp on cl.claim_id = cp.claimId
  where cp.call_success
),

claims_paid AS (
  select
    claim_date,
    cover_asset,
    claim_amount
  from claims_paid_v1
  union all
  select
    claim_date,
    cover_asset,
    claim_amount
  from claims_paid_v2
  --UNION
  --SELECT
  --  CAST('2021-11-05 00:00' AS TIMESTAMP) AS claim_time,
  --  10.43 AS amount,
  --  'ETH' AS cover_asset
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
      claim_time,
      SUM(
        CASE
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'USD' THEN amount * dai_price_dollar
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'ETH' THEN amount * dai_price_dollar / eth_price_dollar
          ELSE 0
        END
      ) OVER (
        ORDER BY
          claim_time
      ) AS running_dai_claimed,
      SUM(
        CASE
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'USD' THEN amount * eth_price_dollar
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'ETH' THEN amount
          ELSE 0
        END
      ) OVER (
        ORDER BY
          claim_time
      ) AS running_eth_claimed,
      SUM(
        CASE
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'USD' THEN amount * eth_price_dollar
          WHEN cover_asset = 'ETH'
          AND '{{display_currency}}' = 'ETH' THEN amount
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'USD' THEN amount * dai_price_dollar
          WHEN cover_asset = 'DAI'
          AND '{{display_currency}}' = 'ETH' THEN amount * dai_price_dollar / eth_price_dollar
          ELSE 0
        END
      ) OVER (
        ORDER BY
          claim_time
      ) AS running_total_claimed
    from claims_paid
      inner join prices on claims_paid.claim_time = prices.time
  )

select
  *
from total_claims
where claim_time >= timestamp '{{Start Date}}'
  and claim_time <= timestamp '{{End Date}}'
order by claim_time desc
