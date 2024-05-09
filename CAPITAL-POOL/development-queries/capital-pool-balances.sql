with

nexusmutual_contracts (contract_address) as (
  values
  (0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60), --Pool (active), deployed: Nov-21-2023
  (0xcafea112Db32436c2390F5EC988f3aDB96870627), --Pool (Pool V2), deployed: Mar-08-2023
  (0xcafea35ce5a2fc4ced4464da4349f81a122fd12b), --Pool (Pool3), deployed: May-25-2021
  (0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8), --Pool (old), deployed: Jan-26-2021
  (0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb), --Pool2 (Pool 4), deployed: Jan-26-2021
  (0xfd61352232157815cf7b71045557192bf0ce1884), --Pool1, deployed: May-23-2019
  (0x7cbe5682be6b648cc1100c76d4f6c96997f753d6), --Pool2, deployed: May-23-2019
  (0xcafea8321b5109d22c53ac019d7a449c947701fb), --MCR, deployed: May-25-2021
  (0xcafea92739e411a4D95bbc2275CA61dE6993C9a7)  --MCR, deployed: Nov-21-2023
),

transfer_in as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    symbol,
    contract_address,
    amount,
    cast(amount_raw as double) as amount_raw,
    amount_usd,
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2019-01-01'
    and "to" in (select contract_address from nexusmutual_contracts)
),

transfer_out as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    symbol,
    contract_address,
    -1 * amount as amount,
    -1 * cast(amount_raw as double) as amount_raw,
    -1 * amount_usd as amount_usd,
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2019-01-01'
    and "from" in (select contract_address from nexusmutual_contracts)
),

transfer_nxmty_in as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'NXMTY' as symbol,
    contract_address,
    cast(amount_raw as double) as amount_raw,
    tx_hash
  from tokens_ethereum.transfers
  where block_time > timestamp '2019-01-01'
    and "to" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
),

transfer_nxmty_out as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'NXMTY' as symbol,
    contract_address,
    -1 * cast(amount_raw as double) as amount_raw,
    tx_hash
  from tokens_ethereum.transfers
  where block_time > timestamp '2019-01-01'
    and "from" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
),

transfer_combined as (
  select block_time, block_date, symbol, contract_address, amount, amount_usd
  from (
    select block_time, block_date, symbol, contract_address, amount, amount_usd
    from transfer_in
    union all
    select block_time, block_date, symbol, contract_address, amount, amount_usd
    from transfer_out
    union all
    select block_time, block_date, symbol, contract_address, amount_raw / 1e18 as amount, -1.00 as amount_usd -- dummy usd amount
    from transfer_nxmty_in
    union all
    select block_time, block_date, symbol, contract_address, amount_raw / 1e18 as amount, -1.00 as amount_usd -- dummy usd amount
    from transfer_nxmty_out
  ) t
),

lido_oracle as (
  select
    1 as anchor,
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    cast(postTotalPooledEther as double) / cast(totalShares as double) as rebase
  from lido_ethereum.LegacyOracle_evt_PostTotalShares
  where evt_block_time >= timestamp '2021-05-26'
),

steth_adjusted_date as (
  select
    date_add('day', case when t.block_time < lo.block_time then -1 else 0 end, t.block_date) as block_date,
    t.amount as steth_amount
  from lido_oracle lo
    inner join transfer_combined t on lo.block_date = t.block_date
  where t.symbol = 'stETH'
),

steth_net_staking as (
  select
    1 as anchor,
    lo.block_date,
    sd.steth_amount,
    lo.rebase as rebase2
  from lido_oracle lo
    inner join steth_adjusted_date sd on lo.block_date = sd.block_date
),

steth_expanded_rebase as (
  select
    lo.block_date,
    ns.steth_amount,
    lo.rebase,
    ns.rebase2
  from steth_net_staking ns
    inner join lido_oracle lo on ns.anchor = lo.anchor
  where ns.block_date <= lo.block_date
),

steth_running_total as (
  select distinct
    block_date,
    sum(steth_amount * rebase / rebase2) over (partition by block_date) as steth_total
  from steth_expanded_rebase
),

chainlink_oracle_nxmty_price as (
  select
    block_date,
    avg(oracle_price) as nxmty_price
  from chainlink_ethereum.price_feeds
  where proxy_address = 0xcc72039a141c6e34a779ef93aef5eb4c82a893c7 -- Nexus wETH Reserves
    and block_time > timestamp '2022-08-15'
  group by 1
),

nxmty_running_total as (
  select
    cop.block_date,
    sum(t.amount) over (order by cop.block_date) as nxmty_total,
    sum(t.amount) over (order by cop.block_date) * cop.nxmty_price as nxmty_in_eth_total
  from chainlink_oracle_nxmty_price cop
    left join (
      select block_date, sum(amount) as amount
      from transfer_combined
      where symbol = 'NXMTY'
      group by 1
    ) t on cop.block_date = t.block_date
),

combined_running_total as (
  select
    block_date,
    sum(case when symbol = 'ETH' then amount end) over (order by block_date) as eth_total,
    sum(case when symbol = 'DAI' then amount end) over (order by block_date) as dai_total,
    sum(case when symbol = 'rETH' then amount end) over (order by block_date) as reth_total,
    sum(case when symbol = 'USDC' then amount end) over (order by block_date) as usdc_total
  from (
    select
      block_date,
      symbol,
      sum(amount) as amount,
      sum(amount_usd) as amount_usd
    from transfer_combined
    where symbol in ('ETH', 'DAI', 'rETH', 'USDC')
    group by 1,2
  ) t
),

day_sequence as (
  select cast(d.seq_date as date) as block_date
  from (select sequence(date '2019-01-01', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

all_running_totals as (
  select
    ds.block_date,
    coalesce(ct.eth_total, lag(ct.eth_total) over (order by ds.block_date), 0) as eth_running_total,
    coalesce(ct.dai_total, lag(ct.dai_total) over (order by ds.block_date), 0) as dai_running_total,
    coalesce(ct.reth_total, lag(ct.reth_total) over (order by ds.block_date), 0) as reth_running_total,
    coalesce(ct.usdc_total, lag(ct.usdc_total) over (order by ds.block_date), 0) as usdc_running_total,
    coalesce(rt.steth_total, lag(rt.steth_total) over (order by ds.block_date), 0) as steth_running_total,
    coalesce(nt.nxmty_total, lag(nt.nxmty_total) over (order by ds.block_date), 0) as nxmty_running_total,
    coalesce(nt.nxmty_in_eth_total, lag(nt.nxmty_in_eth_total) over (order by ds.block_date), 0) as nxmty_in_eth_running_total
  from day_sequence ds
    left join combined_running_total ct on ds.block_date = ct.block_date
    left join steth_running_total rt on ds.block_date = rt.block_date
    left join nxmty_running_total nt on ds.block_date = nt.block_date
)

select
  block_date,
  eth_running_total,
  dai_running_total,
  reth_running_total,
  usdc_running_total,
  steth_running_total,
  nxmty_running_total,
  nxmty_in_eth_running_total
from all_running_totals
--where block_date >= timestamp '2021-10-04' -- 15286.709696721391
--where block_date >= timestamp '2021-05-26'
--where block_date >= timestamp '2022-08-15'
order by 1 desc
limit 10


/*
select
  symbol,
  contract_address,
  sum(amount) as amount,
  sum(amount_usd) as amount_usd
from transfer_combined
--where symbol <> 'SAI'
group by 1,2
having abs(sum(amount)) >= 0.0001
  and abs(sum(amount_usd)) > 0.99
order by 1
*/
