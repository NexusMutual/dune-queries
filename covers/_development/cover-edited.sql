--select * from nexusmutual_ethereum.cover_evt_coveredited order by evt_block_time desc

select
  evt_block_time as block_time,
  buyer,
  productId as product_id,
  amount,
  coverId as cover_id,
  originalCoverId as original_cover_id,
  evt_tx_hash as tx_hash
from nexusmutual_ethereum.cover_evt_coverbought
--where coverId <> originalCoverId
order by 1
