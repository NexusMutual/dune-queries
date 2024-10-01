select
  pool_id,
  pool_name,
  product_id,
  product_name,
  product_type,
  case '{{currency}}'
    when 'NXM' then max_capacity_nxm
    when 'ETH' then max_capacity_nxm_eth
    when 'USD' then max_capacity_nxm_usd
  end max_capacity,
  case '{{currency}}'
    when 'NXM' then nxm_cover_amount
    when 'ETH' then eth_cover_amount
    when 'USD' then usd_cover_amount
  end cover_amount,
  case '{{currency}}'
    when 'NXM' then available_capacity_nxm
    when 'ETH' then available_capacity_eth
    when 'USD' then available_capacity_usd
  end available_capacity
from query_3991071 -- staking pools products overview (staked per product)
where (max_capacity_nxm <> 0 or nxm_cover_amount <> 0 or available_capacity_nxm <> 0)
  and pool_id = {{pool_id}}
order by product_id
