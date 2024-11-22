WITH
  nxm_stake AS (
    SELECT
      date_trunc('day', evt_block_time) AS day,
      amount * -1E-18 AS nxm_amount
    FROM
      nexusmutual."PooledStaking_evt_Withdrawn"
    UNION
    SELECT
      date_trunc('day', evt_block_time) AS day,
      amount * 1E-18 AS nxm_amount
    FROM
      nexusmutual."PooledStaking_evt_Deposited"
  ),
  net_nxm_stake AS (
    SELECT
      DISTINCT day,
      SUM(nxm_amount) OVER (PARTITION BY day) AS net_change_allocated,
      SUM(nxm_amount) OVER (
        ORDER BY
          day
      ) AS running_total_staked
    FROM
      nxm_stake
  ),
  nxm_allocated AS (
    SELECT
      date_trunc('day', evt_block_time) AS day,
      amount * 1E-18 AS nxm_amount,
      "contractAddress" AS product_address
    FROM
      nexusmutual."PooledStaking_evt_Staked"
    UNION
    SELECT
      date_trunc('day', evt_block_time) AS day,
      amount * -1E-18 AS nxm_amount,
      "contractAddress" AS product_address
    FROM
      nexusmutual."PooledStaking_evt_Unstaked"
  ),
  net_nxm_allocated AS (
    select
      distinct day,
      SUM(nxm_amount) OVER (PARTITION BY day) as net_change_staked,
      SUM(nxm_amount) OVER (
        ORDER BY
          day
      ) as running_total_allocated
    from
      nxm_allocated
  ),
  allocated_joined_staked AS (
    select
      COALESCE(net_nxm_stake.day, net_nxm_allocated.day) as day,
      running_total_allocated as running_total_allocated,
      running_total_staked as running_total_staked
    from
      net_nxm_allocated
      FULL JOIN net_nxm_stake ON net_nxm_stake.day = net_nxm_allocated.day
    ORDER BY
      day ASC
  ),
  -- Fill gaps in the data set, created by the the full join above, with sub-querys for the last non-null value
  allocated_joined_staked_gap_filled AS (
    select
      day,
      COALESCE(
        running_total_allocated,
        (
          SELECT
            running_total_allocated
          FROM
            allocated_joined_staked as inner_table
          where
            b.day > inner_table.day
            and inner_table.running_total_allocated IS NOT NULL
          ORDER BY
            day desc
          LIMIT
            1
        )
      ) as running_total_allocated,
      COALESCE(
        running_total_staked,
        (
          SELECT
            running_total_staked
          FROM
            allocated_joined_staked as inner_table
          where
            b.day > inner_table.day
            and inner_table.running_total_staked IS NOT NULL
          ORDER BY
            day desc
          LIMIT
            1
        )
      ) as running_total_staked
    from
      allocated_joined_staked as b
  )
SELECT
  day,
  running_total_allocated / running_total_staked as leverage
FROM
  allocated_joined_staked_gap_filled
  WHERE
  day >= '{{1. Start Date}}'
  AND day <= '{{2. End Date}}'
ORDER BY
  day DESC
 