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
    'in' as transfer_type,
    symbol,
    amount,
    contract_address,
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2019-05-01'
    and "to" in (select contract_address from nexusmutual_contracts)
    and symbol in ('ETH', 'DAI', 'stETH', 'rETH', 'USDC')
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
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2019-05-01'
    and "from" in (select contract_address from nexusmutual_contracts)
    and symbol in ('ETH', 'DAI', 'stETH', 'rETH', 'USDC')
),

transfer_nxmty_in as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'in' as transfer_type,
    'NXMTY' as symbol,
    cast(amount_raw as double) / 1e18 as amount,
    contract_address,
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2022-05-27'
    and "to" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
),

transfer_nxmty_out as (
  select
    block_time,
    block_number,
    date_trunc('day', block_time) as block_date,
    'out' as transfer_type,
    'NXMTY' as symbol,
    -1 * cast(amount_raw as double) / 1e18 as amount,
    contract_address,
    tx_hash
  from tokens_ethereum.transfers
  where block_time >= timestamp '2022-05-27'
    and "from" in (select contract_address from nexusmutual_contracts)
    and contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd --NXMTY
),

transfer_combined as (
  select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, tx_hash
  from transfer_in
  union all
  select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, tx_hash
  from transfer_out
  union all
  select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, tx_hash
  from transfer_nxmty_in
  union all
  select block_time, block_number, block_date, transfer_type, symbol, amount, contract_address, tx_hash
  from transfer_nxmty_out
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
    inner join (
      select block_date, sum(steth_amount) as steth_amount
      from steth_adjusted_date
      group by 1
     ) sd on lo.block_date = sd.block_date
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

transfer_totals as (
  select
    block_date,
    sum(case when symbol = 'ETH' then amount end) as eth_total,
    sum(case when symbol = 'DAI' then amount end) as dai_total,
    sum(case when symbol = 'rETH' then amount end) as reth_total,
    sum(case when symbol = 'USDC' then amount end) as usdc_total
  from transfer_combined
  group by 1
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'ETH'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2019-05-01'
  group by 1
),

daily_avg_dai_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'DAI'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2019-11-13'
  group by 1
),

daily_avg_reth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'rETH'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2021-09-30'
  group by 1
),

daily_avg_usdc_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as price_usd
  from prices.usd
  where symbol = 'USDC'
    and coalesce(blockchain, 'ethereum') = 'ethereum'
    and minute >= timestamp '2019-05-01'
  group by 1
),

daily_ma7_eth_prices as (
  select
    block_date,
    avg(price_usd) over (order by block_date rows between 6 preceding and current row) as price_ma7_usd
  from daily_avg_eth_prices
),

daily_ma7_dai_prices as (
  select
    block_date,
    avg(price_usd) over (order by block_date rows between 6 preceding and current row) as price_ma7_usd
  from daily_avg_dai_prices
),

daily_ma7_reth_prices as (
  select
    block_date,
    avg(price_usd) over (order by block_date rows between 6 preceding and current row) as price_ma7_usd
  from daily_avg_reth_prices
),

daily_ma7_usdc_prices as (
  select
    block_date,
    avg(price_usd) over (order by block_date rows between 6 preceding and current row) as price_ma7_usd
  from daily_avg_usdc_prices
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (select sequence(date '2019-05-01', current_date, interval '1' day) as days) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_running_totals as (
  select
    ds.block_date,
    sum(tt.eth_total) over (order by ds.block_date) as eth_total,
    sum(tt.dai_total) over (order by ds.block_date) as dai_total,
    sum(tt.reth_total) over (order by ds.block_date) as reth_total,
    sum(tt.usdc_total) over (order by ds.block_date) as usdc_total,
    coalesce(steth_rt.steth_total, lag(steth_rt.steth_total) over (order by ds.block_date), 0) as steth_total,
    coalesce(nxmty_rt.nxmty_total, lag(nxmty_rt.nxmty_total) over (order by ds.block_date), 0) as nxmty_total,
    coalesce(nxmty_rt.nxmty_in_eth_total, lag(nxmty_rt.nxmty_in_eth_total) over (order by ds.block_date), 0) as nxmty_eth_total
  from day_sequence ds
    left join transfer_totals tt on ds.block_date = tt.block_date
    left join steth_running_total steth_rt on ds.block_date = steth_rt.block_date
    left join nxmty_running_total nxmty_rt on ds.block_date = nxmty_rt.block_date
),

daily_running_totals_enriched as (
  select
    drt.block_date,
    -- ETH
    drt.eth_total,
    drt.eth_total * p_avg_eth.price_usd as avg_eth_usd_total,
    drt.eth_total * p_ma7_eth.price_ma7_usd as ma7_eth_usd_total,
    -- DAI
    drt.dai_total,
    drt.dai_total * p_avg_dai.price_usd as avg_dai_usd_total,
    drt.dai_total * p_ma7_dai.price_ma7_usd as ma7_dai_usd_total,
    drt.dai_total * p_avg_dai.price_usd / p_avg_eth.price_usd as avg_dai_eth_total,
    drt.dai_total * p_ma7_dai.price_ma7_usd / p_ma7_eth.price_ma7_usd as ma7_dai_eth_total,
    -- NXMTY
    drt.nxmty_total,
    drt.nxmty_eth_total,
    drt.nxmty_eth_total * p_avg_eth.price_usd as avg_nxmty_usd_total,
    drt.nxmty_eth_total * p_ma7_eth.price_ma7_usd as ma7_nxmty_usd_total,
    -- stETH
    drt.steth_total,
    drt.steth_total * p_avg_eth.price_usd as avg_steth_usd_total,
    drt.steth_total * p_ma7_eth.price_ma7_usd as ma7_steth_usd_total,
    -- rETH
    drt.reth_total,
    drt.reth_total * p_avg_reth.price_usd as avg_reth_usd_total,
    drt.reth_total * p_ma7_reth.price_ma7_usd as ma7_reth_usd_total,
    drt.reth_total * p_avg_reth.price_usd / p_avg_eth.price_usd as avg_reth_eth_total,
    drt.reth_total * p_ma7_reth.price_ma7_usd / p_ma7_eth.price_ma7_usd as ma7_reth_eth_total,
    -- USDC
    drt.usdc_total,
    drt.usdc_total * p_avg_usdc.price_usd as avg_usdc_usd_total,
    drt.usdc_total * p_ma7_usdc.price_ma7_usd as ma7_usdc_usd_total,
    drt.usdc_total * p_avg_usdc.price_usd / p_avg_eth.price_usd as avg_usdc_eth_total,
    drt.usdc_total * p_ma7_usdc.price_ma7_usd / p_ma7_eth.price_ma7_usd as ma7_usdc_eth_total
  from daily_running_totals drt
    inner join daily_avg_eth_prices p_avg_eth on drt.block_date = p_avg_eth.block_date
    inner join daily_ma7_eth_prices p_ma7_eth on drt.block_date = p_ma7_eth.block_date
    left join daily_avg_dai_prices p_avg_dai on drt.block_date = p_avg_dai.block_date
    left join daily_ma7_dai_prices p_ma7_dai on drt.block_date = p_ma7_dai.block_date
    left join daily_avg_reth_prices p_avg_reth on drt.block_date = p_avg_reth.block_date
    left join daily_ma7_reth_prices p_ma7_reth on drt.block_date = p_ma7_reth.block_date
    left join daily_avg_usdc_prices p_avg_usdc on drt.block_date = p_avg_usdc.block_date
    left join daily_ma7_usdc_prices p_ma7_usdc on drt.block_date = p_ma7_usdc.block_date
)

select
  block_date,
  -- Capital Pool totals
  eth_total + nxmty_eth_total + steth_total + avg_dai_eth_total + avg_reth_eth_total + avg_usdc_eth_total as avg_capital_pool_eth_total,
  eth_total + nxmty_eth_total + steth_total + ma7_dai_eth_total + ma7_reth_eth_total + ma7_usdc_eth_total as ma7_capital_pool_eth_total,
  -- ETH
  eth_total,
  avg_eth_usd_total,
  ma7_eth_usd_total,
  -- DAI
  dai_total,
  avg_dai_usd_total,
  ma7_dai_usd_total,
  avg_dai_eth_total,
  ma7_dai_eth_total,
  -- NXMTY
  nxmty_total,
  nxmty_eth_total,
  avg_nxmty_usd_total,
  ma7_nxmty_usd_total,
  -- stETH
  steth_total,
  avg_steth_usd_total,
  ma7_steth_usd_total,
  -- rETH
  reth_total,
  avg_reth_usd_total,
  ma7_reth_usd_total,
  avg_reth_eth_total,
  ma7_reth_eth_total,
  -- USDC
  usdc_total,
  avg_usdc_usd_total,
  ma7_usdc_usd_total,
  avg_usdc_eth_total,
  ma7_usdc_eth_total
from daily_running_totals_enriched
--where block_date >= timestamp '2021-10-04' -- 15286.709696721391
--where block_date >= timestamp '2021-05-26'
--where block_date >= timestamp '2022-08-15'
order by 1 desc
