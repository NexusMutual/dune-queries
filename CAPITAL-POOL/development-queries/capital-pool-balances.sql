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

dai_transactions AS (
    SELECT
      block_date,
      SUM(amount) AS dai_net_total
    FROM transfer_combined
    WHERE symbol = 'DAI'
    group by 1
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

select * from dai_transactions order by 1
