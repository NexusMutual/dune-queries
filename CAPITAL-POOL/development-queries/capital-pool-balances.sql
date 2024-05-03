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

day_sequence as (
  select cast(d.seq_date as date) as block_date
  from (select sequence(date '2019-01-01', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

transfer_in as (
  select
    date_trunc('day', block_time) as block_date,
    symbol,
    contract_address,
    sum(amount) as amount,
    cast(sum(amount_raw) as double) as amount_raw,
    sum(amount_usd) as amount_usd
  from tokens_ethereum.transfers
  where block_time > timestamp '2019-01-01'
    and "to" in (select contract_address from nexusmutual_contracts)
    /*
    -- exclude transfers between contracts
    and not (("from" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8 and "to" = 0xcafea35ce5a2fc4ced4464da4349f81a122fd12b) -- Pool -> Pool3
          or ("from" = 0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb and "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8) -- Pool2 -> Pool
          or ("from" = 0xfd61352232157815cf7b71045557192bf0ce1884 and "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8) -- Pool1 -> Pool
    )
    */
  group by 1,2,3
),

transfer_out as (
  select
    date_trunc('day', block_time) as block_date,
    symbol,
    contract_address,
    -1 * sum(amount) as amount,
    -1 * cast(sum(amount_raw) as double) as amount_raw,
    -1 * sum(amount_usd) as amount_usd
  from tokens_ethereum.transfers
  where block_time > timestamp '2019-01-01'
    and "from" in (select contract_address from nexusmutual_contracts)
    /*
    -- exclude transfers between contracts
    and not (("from" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8 and "to" = 0xcafea35ce5a2fc4ced4464da4349f81a122fd12b) -- Pool -> Pool3
          or ("from" = 0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb and "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8) -- Pool2 -> Pool
          or ("from" = 0xfd61352232157815cf7b71045557192bf0ce1884 and "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8) -- Pool1 -> Pool
    )
    */
  group by 1,2,3
),

transfer_nxmty_in as (
  select
    date_trunc('day', block_time) as block_date,
    'NXMTY' as symbol,
    contract_address,
    cast(sum(amount_raw) as double) as amount_raw
  from tokens_ethereum.transfers
  where block_time > timestamp '2019-01-01'
    and "to" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
  group by 1,2,3
),

transfer_nxmty_out as (
  select
    date_trunc('day', block_time) as block_date,
    'NXMTY' as symbol,
    contract_address,
    -1 * cast(sum(amount_raw) as double) as amount_raw
  from tokens_ethereum.transfers
  where block_time > timestamp '2019-01-01'
    and "from" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
  group by 1,2,3
),

transfer_combined as (
  select block_date, symbol, contract_address, amount, amount_usd
  from transfer_in
  union all
  select block_date, symbol, contract_address, amount, amount_usd
  from transfer_out
  union all
  select block_date, symbol, contract_address, amount_raw / 1e18 as amount, -1.00 as amount_usd -- dummy usd amount
  from transfer_nxmty_in
  union all
  select block_date, symbol, contract_address, amount_raw / 1e18 as amount, -1.00 as amount_usd -- dummy usd amount
  from transfer_nxmty_out
),

steth as (
  select
    block_date,
    sum(amount) as amount
  from transfer_combined
  where symbol = 'stETH'
  group by 1
),

steth_fill_days as (
  select
    ds.block_date,
    steth.amount,
    row_number() over (order by ds.block_date) as rn
  from day_sequence ds
    left join steth on ds.block_date = steth.block_date
  where ds.block_date >= (select min(block_date) from steth)
),

steth_rebase as (
  select
    date_trunc('day', evt_block_time) as block_date,
    evt_block_time,
    evt_block_number,
    1.0 + (postTotalPooledEther - preTotalPooledEther) / (cast(preTotalPooledEther as double)) * 0.9 as rebase_rate,
    (
      (postTotalPooledEther - preTotalPooledEther) * 365 * 24 * 60 * 60
    ) / cast((preTotalPooledEther * timeElapsed) as double) * 0.9 as staking_APR
  from lido_ethereum.LegacyOracle_evt_PostTotalShares
  where evt_block_time <= cast('2023-05-16 00:00' as timestamp)
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    evt_block_time,
    evt_block_number,
    1.0 + (post_share_rate - pre_share_rate) / cast((pre_share_rate) as double) as rebase_rate, 
    (
      365 * 24 * 60 * 60 * (post_share_rate - pre_share_rate)
    ) / cast((pre_share_rate * timeElapsed) as double) as staking_APR
  from (
      select
        evt_block_time,
        evt_block_number,
        timeElapsed,
        (preTotalEther * 1e27) / cast(preTotalShares as double) as pre_share_rate,
        (postTotalEther * 1e27) / cast(postTotalShares as double) as post_share_rate
      from lido_ethereum.steth_evt_TokenRebased
    ) t
),

steth_ext as (
  select
    r.block_date,
    r.rebase_rate,
    sfd.amount,
    sfd.rn,
    --sum(sfd.amount) over (order by r.block_date) * r.rebase_rate as amount_running,
    --sum(coalesce(s.amount, 0) * r.rebase_rate) over (partition by r.block_date) as amount_rebased,
    sum(sfd.amount * r.rebase_rate) over (order by r.block_date) * if(sfd.rn=1, 1, power(r.rebase_rate, sfd.rn-2)) as running_total
  from steth_fill_days sfd
    inner join steth_rebase r on sfd.block_date = r.block_date
)

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

--/*
select * from steth_ext
--where block_date >= timestamp '2021-05-26'
order by 1
--*/

--select * from steth_fill_days order by rn
