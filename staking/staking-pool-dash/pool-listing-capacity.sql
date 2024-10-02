with

params as (
  select cast(split_part('{{pool}}', ' : ', 1) as int) as pool_id
)

select
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
  end available_capacity,
  available_capacity_nxm / max_capacity_nxm as utilisation
from query_3991071 -- staking pools products overview (staked per product)
where (max_capacity_nxm <> 0 or nxm_cover_amount <> 0 or available_capacity_nxm <> 0)
  and cast(pool_id as int) in (select pool_id from params)
order by product_id
