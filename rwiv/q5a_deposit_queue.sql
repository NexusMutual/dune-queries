-- RWIV Q5a - Deposit Queue (Live)
-- Dune query ID: 6976979
-- Shows only pending deposit requests awaiting VO action.
-- Sorted earliest-first.
-- Subset of Q5 (Deposits).

WITH member_addresses AS (
  SELECT DISTINCT memberId, memberAddress
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
  UNION
  SELECT DISTINCT memberId, memberAddress
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_redeemfulfilled
),

deposit_requests AS (
  SELECT
    requestId,
    memberId,
    CAST(assets AS double) AS assets,
    evt_block_time
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositrequested
),

deposit_fulfillments AS (
  SELECT DISTINCT requestId
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositfulfilled
),

deposit_cancellations AS (
  SELECT DISTINCT requestId
  FROM nexusmutual_rwiv_ethereum.rwivault_evt_depositrequestcanceled
)

SELECT
  dr.evt_block_time AS request_timestamp,
  dr.requestId AS request_id,
  ma.memberAddress AS address,
  ROUND(dr.assets / 1e6, 2) AS request_size_usdc
FROM deposit_requests dr
LEFT JOIN member_addresses ma ON dr.memberId = ma.memberId
WHERE dr.requestId NOT IN (SELECT requestId FROM deposit_fulfillments)
  AND dr.requestId NOT IN (SELECT requestId FROM deposit_cancellations)
ORDER BY dr.evt_block_time ASC
