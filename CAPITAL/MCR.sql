WITH
  MCR_event as (
    select
      "evt_block_time" as date,
      "mcrEtherx100" * 1E-18 as mcr_eth,
      7000 as mcr_floor,
      0 as mcr_cover_min
    from
      nexusmutual."MCR_evt_MCREvent"
  ),
  MCR_updated as (
    select
      "evt_block_time" as date,
      "mcr" * 1E-18 as mcr_eth,
      "mcrFloor" * 1E-18 as mcr_floor,
      "mcrETHWithGear" * 1E-18 as mcr_cover_min
    from
      nexusmutual."MCR_evt_MCRUpdated"
  )
select
  *
from
  MCR_event
UNION
select
  *
from
  MCR_updated