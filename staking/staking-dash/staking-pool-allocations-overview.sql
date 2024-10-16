select
  pool_id,
  pool_name,
  product_id,
  product_name,
  product_type,
  target_weight,
  case '{{currency}}'
    when 'NXM' then staked_nxm
    when 'ETH' then staked_nxm_eth
    when 'USD' then staked_nxm_usd
  end as staked,
  case '{{currency}}'
    when 'NXM' then max_capacity_nxm
    when 'ETH' then max_capacity_nxm_eth
    when 'USD' then max_capacity_nxm_usd
  end max_capacity,
  active_cover_count,
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
from query_3991071 -- staking pools products overview - base query
order by pool_id, product_name
