with all_address as (
select
  t.from as address
from
  nexusmutual."NXMToken_evt_Transfer" as t
UNION
select
  t.to as address
from
  nexusmutual."NXMToken_evt_Transfer" as t
  )
SELECT DISTINCT address from all_address