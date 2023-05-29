SELECT
  DISTINCT date_trunc('day', "call_block_time") as date_c,
  CASE
    when "asset" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' then 'ETH'
    WHEN "asset" = '\x6b175474e89094c44da98b954eedeac495271d0f' then 'DAI'
    ELSE 'err'
  end as asset,
  AVG("output_tokenPrice") OVER (PARTITION by "call_block_time", "asset")
from
  nexusmutual."Pool_call_getTokenPrice"
ORDER BY
  date_c desc