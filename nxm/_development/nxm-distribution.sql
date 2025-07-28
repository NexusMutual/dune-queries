with

item_mapping (address_label, item_type) as (
  values
  ('wNXM', 'wNXM'),
  ('TokenController', 'staked'),
  ('TokenController', 'rewards'),
  ('other', 'other')
),

nxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    'out' as transfer_type,
    "from" as address,
    -1 * (value / 1e18) as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    'in' as transfer_type,
    "to" as address,
    value / 1e18 as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
),

nxm_holders as (
  select
    address,
    sum(amount) as amount
  from nxm_transfer
  group by 1
  having sum(amount) > 1e-11 -- assumed "0"
),

nxm_holders_labelled as (
  select
    case
      when address = 0x0d438f3b5175bebc262bf23753c1e53d03432bde then 'wNXM'
      when address = 0x5407381b6c251cfd498ccd4a1d877739cb7960b8 then 'TokenController'
      --when address = 0xcafeaa5f9c401b7295890f309168bbb8173690a3 then 'Assessment'
      --when address in (0x1337def1fc06783d4b03cb8c1bf3ebf7d0593fc4, 0x1337def1e9c7645352d93baf0b789d04562b4185) then 'armor.fi'
      else 'other'
    end as address_label,
    sum(amount) as amount
  from nxm_holders
  group by 1
),

nxm_staked as (
  select sum(total_staked_nxm) as total_staked
  from query_3599009 -- staking pools overview - base query
)

select
  hl.address_label,
  im.item_type,
  case
    when im.item_type = 'staked' then s.total_staked
    when im.item_type = 'rewards' then hl.amount - s.total_staked
    else hl.amount
  end as amount
from item_mapping im
  inner join nxm_holders_labelled hl on im.address_label = hl.address_label
  cross join nxm_staked s
order by amount desc
