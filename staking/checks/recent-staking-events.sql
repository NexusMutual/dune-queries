select
  flow_type,
  block_time,
  --block_date,
  --block_number,
  pool_id,
  --pool_address,
  token_id,
  tranche_id,
  init_tranche_id,
  new_tranche_id,
  tranche_expiry_date,
  is_active,
  amount,
  topup_amount,
  user,
  evt_index,
  tx_hash
from query_5734582 -- staking events - base root
--where pool_id = 22
order by block_time desc
limit 25
