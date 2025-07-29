select
  block_time,
  case
    when tx_hash = 0xfd63fbcc07f95591636f316a84a4ba3fe07c2002df32afac9de640187e491e53 then 'TGE'
    when tx_to = 'NM: PooledStakingProxy' then 'NM staking v1'
    when tx_to = 'NM: RAMM' then 'RAMM'
    when tx_from like 'Hacker:%' or transfer_to like 'Hacker:%' then 'hack'
    when tx_to like 'NM:%' then 'NM protocol'
    when tx_to like '% : %' then 'NM staking v2'
    when tx_to like 'dex:%' then 'dex'
    else 'transfer'
  end as transfer_type,
  tx_from,
  tx_to,
  transfer_from,
  transfer_to,
  amount,
  concat(
    '<a href="https://etherscan.io/tx/', cast(tx_hash as varchar), '" target="_blank">👉 ',
    concat(substring(cast(tx_hash as varchar), 1, 6), '..', substring(cast(tx_hash as varchar), length(cast(tx_hash as varchar)) - 3, 4)),
    ' 🔗</a>'
  ) as tx_link
from query_5531182 -- nxm transfers - base
order by 1
