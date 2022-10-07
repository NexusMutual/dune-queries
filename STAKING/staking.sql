
WITH
  nxm_stake as (
    select
      date_trunc('day', evt_block_time) as day,
      -1 * amount * 1E-18 as nxm_amount
    from
      nexusmutual."PooledStaking_evt_Withdrawn"
    UNION
    select
      date_trunc('day', evt_block_time) as day,
      amount * 1E-18 as nxm_amount
    from
      nexusmutual."PooledStaking_evt_Deposited"
  ),
  net_nxm_stake as (
    select
      day,
      SUM(nxm_amount) as total_nxm_amount
    from
      nxm_stake
    GROUP BY
      day
  )
select
  day,
  SUM(nxm_amount) over (
    order by
      day asc rows between unbounded preceding
      and current row
  ) as total_nxm
from
  nxm_stake
ORDER BY
  day