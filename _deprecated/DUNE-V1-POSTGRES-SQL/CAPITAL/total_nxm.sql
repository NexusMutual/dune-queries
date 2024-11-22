with
  minted_nxm as (
    select
      date_trunc('day', evt_block_time) as day,
      sum(value / 1e18) as NXM_supply_minted
    from
      nexusmutual."NXMToken_evt_Transfer" t
    where
      t.from = '\x0000000000000000000000000000000000000000'
    GROUP by
      day
    ORDER BY
      day
  ),
  burnt_nxm as (
    select
      date_trunc('day', evt_block_time) as day,
      sum(-1 * value / 1e18) as NXM_supply_burnt
    from
      nexusmutual."NXMToken_evt_Transfer" t
    where
      t.to = '\x0000000000000000000000000000000000000000'
    GROUP by
      day
    ORDER BY
      day
  ),
  minted_burnt_nxm as (
    select
      CASE
        WHEN minted_nxm.day is NULL THEN burnt_nxm.day
        ELSE minted_nxm.day
      END as day,
      CASE
        WHEN NXM_supply_minted is NULL THEN 0
        ELSE NXM_supply_minted
      END as NXM_supply_minted,
      CASE
        WHEN NXM_supply_burnt is NULL THEN 0
        ELSE NXM_supply_burnt
      END as NXM_supply_burnt
    from
      minted_nxm
      FULL JOIN burnt_nxm ON minted_nxm.day = burnt_nxm.day
  )
SELECT
  day,
  NXM_supply_minted + NXM_supply_burnt AS net_nxm,
  sum(NXM_supply_minted + NXM_supply_burnt) over (
    order by
      day asc rows between unbounded preceding
      and current row
  ) as total_nxm
FROM
  minted_burnt_nxm
WHERE
  day >= '{{Start Date}}'
  AND day <= '{{End Date}}'
ORDER BY
  day DESC