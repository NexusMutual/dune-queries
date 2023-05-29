WITH
  v1_product_info as (
    SELECT
      product_contract_address,
      product_name,
      product_type,
      syndicate
    FROM
      nexusmutual_ethereum.product_information
  ),
  staked as (
    select
      date_trunc('day', evt_block_time) as ts,
      contractAddress as product_address,
      amount * 1E-18 as staked_amount
    from
      nexusmutual_ethereum.PooledStaking_evt_Staked
  ),
  unstaked as (
    select
      date_trunc('day', evt_block_time) as ts,
      contractAddress as product_address,
      amount * 1E-18 as unstaked_amount
    from
      nexusmutual_ethereum.PooledStaking_evt_Unstaked
  ),
  net_staked AS (
    SELECT DISTINCT
      COALESCE(unstaked.ts, staked.ts) AS ts,
      COALESCE(unstaked.product_address, staked.product_address) AS product_address,
      COALESCE(unstaked_amount, 0) as unstaked_amount,
      COALESCE(staked_amount, 0) as staked_amount,
      COALESCE(staked_amount, 0) - COALESCE(unstaked_amount, 0) as net_staked_amount
    FROM
      staked
      FULL JOIN unstaked ON staked.ts = unstaked.ts
      AND staked.product_address = unstaked.product_address
  )
SELECT
DISTINCT
  net_staked.product_address,
  product_name,
  product_type,
  syndicate,
  SUM(net_staked_amount) OVER (
    PARTITION BY syndicate
  ) AS net_staked_amount_per_syndicate,
  SUM(net_staked_amount) OVER (
    PARTITION BY product_type
  ) AS net_staked_amount_per_product_type,
  SUM(net_staked_amount) OVER (
    PARTITION BY product_address
  ) AS net_staked_amount_per_product_name
FROM
  net_staked
LEFT JOIN v1_product_info ON v1_product_info.product_contract_address = net_staked.product_address
