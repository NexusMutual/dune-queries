select
  c.*,
  t.tx_fee_native,
  t.tx_fee_usd,
  t.gas_price_gwei,
  t.gas_price_usd,
  t.gas_used,
  t.transaction_type
from gas_ethereum.fees t
  inner join nexusmutual_ethereum.covers_v1 c on t.block_number = c.block_number
    and t.block_time = c.block_time
    and t.tx_hash = c.tx_hash
where t.block_time >= timestamp '2019-07-12'
