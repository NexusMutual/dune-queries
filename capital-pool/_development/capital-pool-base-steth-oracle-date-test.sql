with

nexusmutual_contracts (contract_address) as (
  values
  (0xcafeaf6eA90CB931ae43a8Cf4B25a73a24cF6158), --Pool (active), deployed: Oct-03-2024
  (0xcafeaBED7e0653aFe9674A3ad862b78DB3F36e60), --Pool, deployed: Nov-21-2023
  (0xcafea112Db32436c2390F5EC988f3aDB96870627), --Pool (Pool V2), deployed: Mar-08-2023
  (0xcafea35ce5a2fc4ced4464da4349f81a122fd12b), --Pool (Pool3), deployed: May-25-2021
  (0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8), --Pool (old), deployed: Jan-26-2021
  (0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb), --Pool2 (Pool 4), deployed: Jan-26-2021
  (0xfd61352232157815cf7b71045557192bf0ce1884), --Pool1, deployed: May-23-2019
  (0x7cbe5682be6b648cc1100c76d4f6c96997f753d6), --Pool2, deployed: May-23-2019
  (0xcafea8321b5109d22c53ac019d7a449c947701fb), --MCR, deployed: May-25-2021
  (0xcafea92739e411a4D95bbc2275CA61dE6993C9a7), --MCR, deployed: Nov-21-2023
  (0x51ad1265C8702c9e96Ea61Fe4088C2e22eD4418e), --Advisory Board multisig
  (0xcafeaed98d7Fce8F355C03c9F3507B90a974f37e)  --SwapOperator
),

transfer_in as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'in' as transfer_type,
    symbol,
    amount,
    contract_address,
    unique_key,
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2019-05-01'
    and "to" in (select contract_address from nexusmutual_contracts)
    and symbol in ('ETH', 'stETH')
),

transfer_out as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'out' as transfer_type,
    symbol,
    -1 * amount as amount,
    contract_address,
    unique_key,
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2019-05-01'
    and "from" in (select contract_address from nexusmutual_contracts)
    and symbol in ('ETH', 'stETH')
),

transfer_combined as (
  select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, unique_key, tx_hash
  from transfer_in
  union all
  select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, unique_key, tx_hash
  from transfer_out
),

transfer_totals as (
  select
    block_date,
    sum(case when symbol = 'ETH' then amount end) as eth_total,
    sum(case when symbol = 'stETH' then amount end) as steth_total
  from transfer_combined
  group by 1
),

lido_oracle as (
  select
    1 as anchor,
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    cast(postTotalPooledEther as double) / cast(totalShares as double) as rebase
  from lido_ethereum.LegacyOracle_evt_PostTotalShares
  where evt_block_time >= timestamp '2021-05-26'
    and evt_block_time < timestamp '2023-05-16'
  union all
  select
    1 as anchor,
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    cast(postTotalEther as double) / cast(postTotalShares as double) as rebase
  from lido_ethereum.steth_evt_TokenRebased
  where evt_block_time >= timestamp '2023-05-16'
),

steth_adjusted_date as (
  select
    t.block_date
    date_add('day', case when t.block_time < lo.block_time then -1 else 0 end, t.block_date) as block_oracle_date,
    t.amount as steth_amount
  from lido_oracle lo
    inner join transfer_combined t on lo.block_date = t.block_date
  where t.symbol = 'stETH'
),

steth_net_staking as (
  select
    1 as anchor,
    sd.block_date,
    sd.steth_amount,
    lo.rebase as rebase2
  from lido_oracle lo
    inner join (
      select block_date, block_oracle_date, sum(steth_amount) as steth_amount
      from steth_adjusted_date
      group by 1, 2
     ) sd on lo.block_date = sd.block_oracle_date
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

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2019-05-23', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_running_totals as (
  select
    ds.block_date,
    sum(coalesce(tt.eth_total, 0)) over (order by ds.block_date) as eth_total,
    sum(coalesce(tt.steth_total, 0)) over (order by ds.block_date) as steth_transfer_total,
    coalesce(
      steth_rt.steth_total,
      lag(steth_rt.steth_total, 1) over (order by ds.block_date),
      lag(steth_rt.steth_total, 2) over (order by ds.block_date),
      0
    ) as steth_total
  from day_sequence ds
    left join transfer_totals tt on ds.block_date = tt.block_date
    left join steth_running_total steth_rt on ds.block_date = steth_rt.block_date
),

daily_running_totals_enriched as (
  select
    drt.block_date,
    coalesce(drt.eth_total, 0) as eth_total,
    coalesce(drt.steth_transfer_total, 0) as steth_transfer_total,
    coalesce(drt.steth_total, 0) as steth_total
  from daily_running_totals drt
)

select
  block_date,
  eth_total,
  steth_transfer_total,
  steth_total
from daily_running_totals_enriched
order by 1 desc
