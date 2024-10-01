select
  pool_id,
  pool_name,
  case '{{currency}}'
    when 'NXM' then sum(max_capacity_nxm)
    when 'ETH' then sum(max_capacity_nxm_eth)
    when 'USD' then sum(max_capacity_nxm_usd)
  end as max_capacity,
  case '{{currency}}'
    when 'NXM' then sum(nxm_cover_amount)
    when 'ETH' then sum(eth_cover_amount)
    when 'USD' then sum(usd_cover_amount)
  end as cover_amount,
  case '{{currency}}'
    when 'NXM' then sum(available_capacity_nxm)
    when 'ETH' then sum(available_capacity_eth)
    when 'USD' then sum(available_capacity_usd)
  end as available_capacity
from query_3991071 -- staking pools products overview (staked per product)
group by 1, 2
having sum(max_capacity_nxm) <> 0 or sum(nxm_cover_amount) <> 0 or sum(available_capacity_nxm) <> 0
order by 1
