WITH
  gross_staked as (
    select
      staker as staker,
      amount * 1E-18 as gross_staked_amount
    from
      nexusmutual_ethereum.PooledStaking_evt_Staked
    UNION
    select
      staker,
      amount * -1E-18 as gross_staked_amount
    from
      nexusmutual_ethereum.PooledStaking_evt_Unstaked
  ),
  net_staked AS (
    SELECT
      DISTINCT staker,
      SUM(gross_staked_amount) OVER (
        PARTITION BY
          staker
      ) as net_nxm_staked
    FROM
      gross_staked
  )
SELECT
  *
FROM
  net_staked
WHERE
  net_nxm_staked > 0