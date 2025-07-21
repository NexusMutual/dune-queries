with

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

wnxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    'out' as transfer_type,
    "from" as address,
    -1 * (value / 1e18) as amount
  from wnxm_ethereum.wnxm_evt_transfer
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    'in' as transfer_type,
    "to" as address,
    value / 1e18 as amount
  from wnxm_ethereum.wnxm_evt_transfer
),

wnxm_holders as (
  select
    address,
    sum(amount) as amount
  from wnxm_transfer
  group by 1
  having sum(amount) > 1e-13 -- assumed "0"
),

holders as (
  select
    coalesce(nxm.address, wnxm.address) as address,
    nxm.amount as nxm_amount,
    nxm.amount / (select sum(amount) from nxm_holders) as nxm_total_supply_pct,
    wnxm.amount as wnxm_amount,
    wnxm.amount / (select sum(amount) from wnxm_holders) as wnxm_total_supply_pct,
    coalesce(nxm.amount, 0) + coalesce(wnxm.amount, 0) as total_amount
  from nxm_holders nxm
    full outer join wnxm_holders wnxm on nxm.address = wnxm.address
),

labels_contracts as (
  select
    address,
    case
      when lower(namespace) = 'wnxm' then name
      when lower(namespace) in ('gnosis_safe', 'gnosissafe', 'gnosis_multisig') then null -- 'gnosis_safe'
      else concat(namespace, ': ', name)
    end as contract_name
  from (
    select
      address, namespace, name,
      row_number() over (partition by address order by created_at desc) as rn
    from ethereum.contracts
    where namespace <> 'safe_test'
  ) t
  where rn = 1
)

select
  h.address,
  coalesce(le.name, lc.contract_name) as address_label,
  if(h.nxm_amount < 1e-6, 0, h.nxm_amount) as nxm_amount,
  if(h.nxm_total_supply_pct < 1e-6, 0, h.nxm_total_supply_pct) as nxm_total_supply_pct,
  if(h.wnxm_amount < 1e-6, 0, h.wnxm_amount) as wnxm_amount,
  if(h.wnxm_total_supply_pct < 1e-6, 0, h.wnxm_total_supply_pct) as wnxm_total_supply_pct,
  if(h.total_amount < 1e-6, 0, h.total_amount) as total_amount
from holders h
  left join labels_contracts lc on h.address = lc.address
  left join labels.ens le on h.address = le.address
order by h.nxm_amount desc
