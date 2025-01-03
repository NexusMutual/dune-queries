with

nxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    if("from" = 0x0000000000000000000000000000000000000000, 'mint', 'burn') as transfer_type,
    if("from" = 0x0000000000000000000000000000000000000000, 1, -1) * (value / 1e18) as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
  where "from" = 0x0000000000000000000000000000000000000000
    or "to" = 0x0000000000000000000000000000000000000000
),

nxm_supply as (
  select
    block_date,
    coalesce(sum(amount) filter (where transfer_type = 'mint'), 0) as nxm_mint,
    coalesce(sum(amount) filter (where transfer_type = 'burn'), 0) as nxm_burn
  from nxm_transfer
  group by 1
)

select
  block_date,
  nxm_mint,
  nxm_burn,
  sum(nxm_mint) over (order by block_date) as total_nxm_mint,
  sum(nxm_burn) over (order by block_date) as total_nxm_burn,
  sum(nxm_mint + nxm_burn) over (order by block_date) as total_nxm
from nxm_supply
--order by 1 desc
