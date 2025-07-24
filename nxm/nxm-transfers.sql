with

labels (address, address_label) as (
  values
    (0x0d438f3b5175bebc262bf23753c1e53d03432bde, 'wNXM'),
    (0x5407381b6c251cfd498ccd4a1d877739cb7960b8, 'NM: TokenController'),
    (0xcafeaa5f9c401b7295890f309168bbb8173690a3, 'NM: Assessment'),
    (0x25783b67b5e29c48449163db19842b8531fdde43, '1confirmation'), -- Nick Tomaino?
    (0xd7cba5b9a0240770cfd9671961dae064136fa240, 'Version One Ventures'),
    (0x963Df0066ff8345922dF88eebeb1095BE4e4e12E, 'NM: Foundation'),
    (0x834b56ecB16D81f75732F633282a280c53BAa0d0, 'Hugh?'), -- linked acc: 0x7adddc4564b89377237d1e4554f7ceddb1c23a02, 116,362 NXM transfer from twiddle.eth to 0x7adddc (tx: 0x204963318fb7ebc75014d02798becd6db8e4ba94227605e2d7abd579cdc8c0b3)
    (0xfa7e852ceb3f7d0f1ac02f3b8afca95e6dcbdb3c, 'NM: DAO Treasury (1)'),
    (0xa179f67882711957307edf3df0c9ee4f63026a12, 'KR1'), -- linked acc: 0x91715128a71c9c734cdc20e5edeeea02e72e428e, initial NXM dist: 110,776
    (0x8e53d04644e9ab0412a8c6bd228c84da7664cfe3, 'NM: DAO Treasury (2)'),
    (0x741aa7cfb2c7bf2a1e7d4da2e3df6a56ca4131f3, '7 Siblings (1)'),
    (0x09abbe423fedee2332caea4117093d7d9b017cf5, 'NM: DAO Treasury (3)'),
    (0x95abc2a62ee543217cf7640b277ba13d056d904a, 'Unity'),
    (0x28a55c4b4f9615fde3cdaddf6cc01fcf2e38a6b0, '7 Siblings (2)'),
    (0x586b9b2f8010b284a0197f392156f1a7eb5e86e9, 'NM: Community Fund'),
    (0x2d089def3b1f95ec8b3052d0d9fa79882554906b, 'Blockchain Capital'),
    (0x28c6c06298d514db089934071355e5743bf21d60, 'Binance 14')
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
    t.tx_from,
    coalesce(l_manual_tx_from.address_label, l_ens_tx_from.name) as tx_from_label,
    t.tx_to,
    coalesce(l_manual_tx_to.address_label, l_ens_tx_to.name) as tx_to_label,
    t.transfer_from,
    coalesce(l_manual_transfer_from.address_label, l_ens_transfer_from.name) as transfer_from_label,
    t.transfer_to,
    coalesce(l_manual_transfer_to.address_label, l_ens_transfer_to.name) as transfer_to_label,
    t.evt_index,
    t.tx_hash
  from nxm_transfers t
    left join labels.ens l_ens_tx_from on t.tx_from = l_ens_tx_from.address
    left join labels l_manual_tx_from on t.tx_from = l_manual_tx_from.address
    left join labels.ens l_ens_tx_to on t.tx_to = l_ens_tx_to.address
    left join labels l_manual_tx_to on t.tx_to = l_manual_tx_to.address
    left join labels.ens l_ens_transfer_from on t.transfer_from = l_ens_transfer_from.address
    left join labels l_manual_transfer_from on t.transfer_from = l_manual_transfer_from.address
    left join labels.ens l_ens_transfer_to on t.transfer_to = l_ens_transfer_to.address
    left join labels l_manual_transfer_to on t.transfer_to = l_manual_transfer_to.address
)

select
  *
from nxm_transfers_labelled
order by 1
