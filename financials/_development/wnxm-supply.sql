with

wnxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    if("from" = 0x0000000000000000000000000000000000000000, 'mint', 'burn') as transfer_type,
    if("from" = 0x0000000000000000000000000000000000000000, 1, -1) * (value / 1e18) as amount
  from wnxm_ethereum.wnxm_evt_transfer
  where "from" = 0x0000000000000000000000000000000000000000
    or "to" = 0x0000000000000000000000000000000000000000
),

wnxm_supply as (
  select
    block_date,
    coalesce(sum(amount) filter (where transfer_type = 'mint'), 0) as wnxm_mint,
    coalesce(sum(amount) filter (where transfer_type = 'burn'), 0) as wnxm_burn
  from wnxm_transfer
  group by 1
)

select
  block_date,
  wnxm_mint,
  wnxm_burn,
  sum(wnxm_mint) over (order by block_date) as total_wnxm_mint,
  sum(wnxm_burn) over (order by block_date) as total_wnxm_burn,
  sum(wnxm_mint + wnxm_burn) over (order by block_date) as total_nxm
from wnxm_supply
--order by 1 desc
