WITH

  capital_pool as (
    select * from query_3773633 -- to be replaced with CP spell
  ),

  display_currency_total as (
    select
      block_date,
      if('{{display_currency}}' = 'USD', avg_capital_pool_usd_total, avg_capital_pool_eth_total) as capital_pool_display_curr
    from capital_pool
  ),

  minted_nxm AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      SUM(CAST(value AS DOUBLE) / 1e18) AS NXM_supply_minted
    FROM
      nexusmutual_ethereum.NXMToken_evt_Transfer AS t
    WHERE
      t."from" = 0x0000000000000000000000000000000000000000
    GROUP BY
      1
    ORDER BY
      day
  ),
  burnt_nxm AS (
    SELECT
      DATE_TRUNC('day', evt_block_time) AS day,
      SUM(-1 * CAST(value AS DOUBLE) / 1e18) AS NXM_supply_burnt
    FROM
      nexusmutual_ethereum.NXMToken_evt_Transfer AS t
    WHERE
      t."to" = 0x0000000000000000000000000000000000000000
    GROUP BY
      1
    ORDER BY
      day
  ),
  minted_burnt_nxm AS (
    SELECT
      CASE
        WHEN minted_nxm.day IS NULL THEN burnt_nxm.day
        ELSE minted_nxm.day
      END AS day,
      CASE
        WHEN NXM_supply_minted IS NULL THEN 0
        ELSE NXM_supply_minted
      END AS NXM_supply_minted,
      CASE
        WHEN NXM_supply_burnt IS NULL THEN 0
        ELSE NXM_supply_burnt
      END AS NXM_supply_burnt
    FROM
      minted_nxm
      FULL JOIN burnt_nxm ON minted_nxm.day = burnt_nxm.day
  ),
  nxm_supply AS (
    SELECT
      day,
      SUM(NXM_supply_minted) OVER (
        ORDER BY
          day
      ) AS total_nxm_minted,
      SUM(NXM_supply_burnt) OVER (
        ORDER BY
          day
      ) AS total_nxm_burned,
      SUM(NXM_supply_minted + NXM_supply_burnt) OVER (
        ORDER BY
          day
      ) AS total_nxm
    FROM
      minted_burnt_nxm
  )

select
  coalesce(display_currency_total.day, nxm_supply.day) as day,
  capital_pool_display_curr,
  total_nxm,
  capital_pool_display_curr / total_nxm as book_value
from nxm_supply
  inner join display_currency_total on display_currency_total.day = nxm_supply.day
where coalesce(display_currency_total.day, nxm_supply.day) >= CAST('{{Start Date}}' AS TIMESTAMP)
  and coalesce(display_currency_total.day, nxm_supply.day) <= CAST('{{End Date}}' AS TIMESTAMP)
order by day desc nulls first
