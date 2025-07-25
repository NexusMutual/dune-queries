with

address_labels as (
  select address, address_label from query_5534312
),

latest_prices as (
  select
    max(block_date) as block_date,
    max_by(avg_nxm_usd_price, block_date) as avg_nxm_usd_price
  from nexusmutual_ethereum.capital_pool_prices
),

nxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    'out' as transfer_type,
    "from" as address,
    -1 * (value / 1e18) as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    'in' as transfer_type,
    "to" as address,
    value / 1e18 as amount
  from nexusmutual_ethereum.NXMToken_evt_Transfer
),

nxm_holders as (
  select
    address,
    sum(amount) as amount
  from nxm_transfer
  group by 1
  having sum(amount) > 1e-11 -- assumed "0"
),

wnxm_transfer as (
  select
    date_trunc('day', evt_block_time) as block_date,
    'out' as transfer_type,
    "from" as address,
    -1 * (value / 1e18) as amount
  from wnxm_ethereum.wnxm_evt_transfer
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    'in' as transfer_type,
    "to" as address,
    value / 1e18 as amount
  from wnxm_ethereum.wnxm_evt_transfer
),

wnxm_holders as (
  select
    address,
    sum(amount) as amount
  from wnxm_transfer
  group by 1
  having sum(amount) > 1e-13 -- assumed "0"
),

staking_mints as (
  select
    call_block_time as block_time,
    poolId as pool_id,
    output_id as token_id,
    "to" as to_address,
    call_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingNFT_call_mint
  where call_success
),

staking_transfers as (
  select
    evt_block_time as block_time,
    tokenId as token_id,
    "from" as from_address,
    "to" as to_address,
    evt_index,
    evt_tx_hash as tx_hash
  from nexusmutual_ethereum.StakingNFT_evt_Transfer
),

stakers as (
  select
    block_time,
    pool_id,
    token_id,
    staker,
    tx_hash,
    row_number() over (partition by pool_id, token_id order by block_time desc, evt_index desc) as pool_token_rn
  from (
    select
      block_time,
      pool_id,
      token_id,
      to_address as staker,
      cast(-1 as bigint) as evt_index,
      tx_hash
    from staking_mints
    union all
    select
      t.block_time,
      m.pool_id,
      m.token_id,
      t.to_address as staker,
      t.evt_index,
      t.tx_hash
    from staking_mints m
      inner join staking_transfers t on m.token_id = t.token_id
    where t.from_address <> 0x0000000000000000000000000000000000000000 -- excl mints
  ) t
),

stakers_active_stake as (
  select
    s.staker,
    s.pool_id,
    s.token_id,
    st.total_staked_nxm as nxm_active_stake
  from stakers s
    inner join query_4079728 st -- staked NXM per token - base
      on s.pool_id = st.pool_id
      and s.token_id = st.token_id
      and s.pool_token_rn = 1
      and (st.token_date_rn = 1 and st.block_date = current_date) -- today's stake
),

stakers_active_stake_total as (
  select
    staker,
    sum(nxm_active_stake) as nxm_active_stake
  from stakers_active_stake
  group by 1
),

holders as (
  select
    coalesce(nxm.address, wnxm.address, stakers.staker) as address,
    nxm.amount as nxm_amount,
    nxm.amount / (select sum(amount) from nxm_holders) as nxm_total_supply_pct,
    wnxm.amount as wnxm_amount,
    wnxm.amount / (select sum(amount) from wnxm_holders) as wnxm_total_supply_pct,
    stakers.nxm_active_stake as nxm_active_stake,
    coalesce(nxm.amount, 0) + coalesce(wnxm.amount, 0) + coalesce(stakers.nxm_active_stake, 0) as total_amount
  from nxm_holders nxm
    full outer join wnxm_holders wnxm on nxm.address = wnxm.address
    full outer join stakers_active_stake_total stakers on coalesce(nxm.address, wnxm.address) = stakers.staker
  where coalesce(nxm.address, wnxm.address) not in (
    0x0d438f3b5175bebc262bf23753c1e53d03432bde, -- wNXM
    0x5407381b6c251cfd498ccd4a1d877739cb7960b8, -- NM: TokenController
    0xcafeaa5f9c401b7295890f309168bbb8173690a3  -- NM: Assessment
  )
),

labels_contracts as (
  select
    address,
    case
      when lower(namespace) = 'wnxm' then name
      when lower(namespace) in ('gnosis_safe', 'gnosissafe', 'gnosis_multisig') then null -- 'gnosis_safe'
      else concat(namespace, ': ', name)
    end as contract_name
  from (
    select
      address, namespace, name,
      row_number() over (partition by address order by created_at desc) as rn
    from ethereum.contracts
    where namespace <> 'safe_test'
  ) t
  where rn = 1
),

holders_enriched as (
  select
    h.address,
    coalesce(al.address_label, le.name, lc.contract_name) as address_label,
    if(h.nxm_amount < 1e-6, 0, h.nxm_amount) as nxm_amount,
    if(h.nxm_total_supply_pct < 1e-6, 0, h.nxm_total_supply_pct) as nxm_total_supply_pct,
    if(h.wnxm_amount < 1e-6, 0, h.wnxm_amount) as wnxm_amount,
    if(h.wnxm_total_supply_pct < 1e-6, 0, h.wnxm_total_supply_pct) as wnxm_total_supply_pct,
    if(h.nxm_active_stake < 1e-6, 0, h.nxm_active_stake) as nxm_active_stake,
    if(h.total_amount < 1e-6, 0, h.total_amount) as total_amount
  from holders h
    left join address_labels al on h.address = al.address
    left join labels_contracts lc on h.address = lc.address
    left join labels.ens le on h.address = le.address
)

select
  he.address,
  he.address_label,
  he.total_amount,
  he.total_amount * lp.avg_nxm_usd_price as total_usd_amount,
  he.nxm_amount,
  he.nxm_amount * lp.avg_nxm_usd_price as nxm_usd_amount,
  he.nxm_total_supply_pct,
  he.wnxm_amount,
  he.wnxm_amount * lp.avg_nxm_usd_price as wnxm_usd_amount,
  he.wnxm_total_supply_pct,
  he.nxm_active_stake,
  he.nxm_active_stake * lp.avg_nxm_usd_price as nxm_active_stake_usd
from holders_enriched he
  cross join latest_prices lp
where he.total_amount > 0
order by he.total_amount desc
