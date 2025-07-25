with

address_labels as (
  select address, address_label from query_5534312
),

nxm_transfers as (
  select
    evt_block_time as block_time,
    evt_block_number as block_number,
    evt_tx_from as tx_from,
    evt_tx_to as tx_to,
    "from" as transfer_from,
    "to" as transfer_to,
    value / 1e18 as amount,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.NXMToken_evt_Transfer
),

nxm_transfers_labelled as (
  select
    t.block_time,
    t.block_number,
    t.amount,
    coalesce(l_manual_tx_from.address_label, l_ens_tx_from.name, cast(t.tx_from as varchar)) as tx_from,
    coalesce(l_manual_tx_to.address_label, l_ens_tx_to.name, cast(t.tx_to as varchar)) as tx_to,
    coalesce(l_manual_transfer_from.address_label, l_ens_transfer_from.name, cast(t.transfer_from as varchar)) as transfer_from,
    coalesce(l_manual_transfer_to.address_label, l_ens_transfer_to.name, cast(t.transfer_to as varchar)) as transfer_to,
    t.evt_index,
    t.tx_hash
  from nxm_transfers t
    left join labels.ens l_ens_tx_from on t.tx_from = l_ens_tx_from.address
    left join address_labels l_manual_tx_from on t.tx_from = l_manual_tx_from.address
    left join labels.ens l_ens_tx_to on t.tx_to = l_ens_tx_to.address
    left join address_labels l_manual_tx_to on t.tx_to = l_manual_tx_to.address
    left join labels.ens l_ens_transfer_from on t.transfer_from = l_ens_transfer_from.address
    left join address_labels l_manual_transfer_from on t.transfer_from = l_manual_transfer_from.address
    left join labels.ens l_ens_transfer_to on t.transfer_to = l_ens_transfer_to.address
    left join address_labels l_manual_transfer_to on t.transfer_to = l_manual_transfer_to.address
)

select
  block_time,
  block_number,
  tx_from,
  tx_to,
  transfer_from,
  transfer_to,
  amount,
  evt_index,
  tx_hash
from nxm_transfers_labelled
order by 1
