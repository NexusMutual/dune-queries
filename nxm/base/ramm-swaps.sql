select
  block_time,
  block_number,
  date_trunc('day', block_time) as block_date,
  'below' as flow_type,
  transfer_from as user,
  amount, -- NXM burned (technically negative)
  tx_hash
from query_5531182 -- nxm transfers - base
where tx_to = 'NM: RAMM'
  and transfer_to = 'origincity.eth' -- burn address for swaps below
union all
select
  block_time,
  block_number,
  date_trunc('day', block_time) as block_date,
  'above' as flow_type,
  transfer_to as user,
  -1 * amount as amount, -- NXM minted (technically positive)
  tx_hash
from query_5531182 -- nxm transfers - base
where tx_to = 'NM: RAMM'
  and transfer_from = 'origincity.eth' -- burn address for swaps above
