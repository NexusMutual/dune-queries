with 

staking_pools as (
  select distinct pool_id, pool_address from nexusmutual_ethereum.staking_pools
),

test as (
  select
    evt_block_time as block_time,
    contract_address as pool_address,
    ipfsDescriptionHash,
    json_parse(http_get(concat('https://api.nexusmutual.io/ipfs/', ipfsDescriptionHash))) as pool_data,
    row_number() over (partition by contract_address order by evt_block_time desc) as rn
  from nexusmutual_ethereum.StakingPool_evt_PoolDescriptionSet
)
select
  t.block_time,
  sp.pool_id,
  t.pool_address,
  json_extract_scalar(t.pool_data, '$.poolName') as pool_name
from test t
  inner join staking_pools sp on t.pool_address = sp.pool_address
where t.rn = 1
order by 2
