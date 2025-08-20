select
  block_time,
  block_number,
  date_trunc('day', block_time) as block_date,
  'below' as flow_type,
  transfer_from as user,
  case
    when starts_with(transfer_from, '0x') then concat(substring(transfer_from, 1, 6), '..', substring(transfer_from, length(transfer_from) - 5, 6))
    else transfer_from
  end as user_formatted,
  amount, -- NXM burned (technically negative)
  tx_hash
from query_5531182 -- nxm transfers - base
where tx_to = 'NM: RAMM'
  and transfer_to = 'null [0x0000..0000]' -- burn address for swaps below
union all
select
  block_time,
  block_number,
  date_trunc('day', block_time) as block_date,
  'above' as flow_type,
  transfer_to as user,
  case
    when starts_with(transfer_to, '0x') then concat(substring(transfer_to, 1, 6), '..', substring(transfer_to, length(transfer_to) - 5, 6))
    else transfer_to
  end as user_formatted,
  -1 * amount as amount, -- NXM minted (technically positive)
  tx_hash
from query_5531182 -- nxm transfers - base
where tx_to = 'NM: RAMM'
  and transfer_from = 'null [0x0000..0000]' -- burn address for swaps above
