with
  nxm_sent as (
    select
      t.from as address,
      SUM(-1 * value * 1E-18) as total_sent
    from
      nexusmutual."NXMToken_evt_Transfer" as t
    GROUP BY
      address
  ),
  nxm_recv as (
    select
      t.to as address,
      SUM(value * 1E-18) as total_recv
    from
      nexusmutual."NXMToken_evt_Transfer" as t
    GROUP BY
      address
  )
SELECT
  nxm_recv.address,
  total_sent,
  total_recv,
  total_sent + total_recv as total
FROM
  nxm_sent
  JOIN nxm_recv ON nxm_recv.address = nxm_sent.address
ORDER BY
  total DESC