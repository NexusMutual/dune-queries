with

deposits as (
  select
    max(block_time) as block_time,
    pool_address,
    token_id,
    tranche_id,
    tranche_id as new_tranche_id,
    sum(amount) as amount,
    max_by(is_active, block_time) as is_active,
    max_by(evt_index, block_time) as evt_index,
    max_by(tx_hash, block_time) as tx_hash,
    1 as chain_level
  from query_3609519
  where flow_type = 'deposit'
  group by 2,3,4
),

deposit_ext as (
  select
    pool_address,
    token_id,
    init_tranche_id,
    new_tranche_id,
    coalesce(topup_amount, 0) as amount,
    block_time,
    is_active,
    evt_index,
    tx_hash
  from query_3609519
  where flow_type = 'deposit extended'
),

deposits_d1 as (
  select
    d.block_time,
    d.pool_address,
    d.token_id,
    d.tranche_id,
    de.new_tranche_id,
    d.amount + de.amount as amount,
    de.is_active,
    de.evt_index,
    de.tx_hash,
    2 as chain_level
  from deposits d
    inner join deposit_ext de on d.pool_address = de.pool_address
      and d.token_id = de.token_id
      and d.tranche_id = de.init_tranche_id
),

deposits_d2 as (
  select
    d.block_time,
    d.pool_address,
    d.token_id,
    d.tranche_id,
    de.new_tranche_id,
    d.amount + de.amount as amount,
    de.is_active,
    de.evt_index,
    de.tx_hash,
    3 as chain_level
  from deposits_d1 d
    inner join deposit_ext de on d.pool_address = de.pool_address
      and d.token_id = de.token_id
      and d.new_tranche_id = de.init_tranche_id
),

deposits_d3 as (
  select
    d.block_time,
    d.pool_address,
    d.token_id,
    d.tranche_id,
    de.new_tranche_id,
    d.amount + de.amount as amount,
    de.is_active,
    de.evt_index,
    de.tx_hash,
    4 as chain_level
  from deposits_d2 d
    inner join deposit_ext de on d.pool_address = de.pool_address
      and d.token_id = de.token_id
      and d.new_tranche_id = de.init_tranche_id
),

depostis_combined as (
  select * from deposits
  union all
  select * from deposits_d1
  union all
  select * from deposits_d2
  union all
  select * from deposits_d3
),

test as (
select
  block_time,
  pool_address,
  token_id,
  tranche_id as init_tranche_id,
  new_tranche_id as current_tranche_id,
  amount as total_amount,
  is_active,
  evt_index,
  tx_hash
from (
    select
      *,
      row_number() over (partition by pool_address, token_id, tranche_id order by chain_level desc) as rn
    from depostis_combined
  ) t
where rn = 1
)

-- compare:
select count(*), sum(total_amount) from test
union all
select count(*), sum(total_amount) from nexusmutual_ethereum.staking_deposit_extensions
