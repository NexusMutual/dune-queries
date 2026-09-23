with

base as (
  select
    block_date,
    nxmty_total as pool_nxmty_units,
    nxmty_eth_total,
    nxmty_eth_total / nullif(nxmty_total, 0) as nxmty_price_eth
  from query_4627588 -- Capital Pool - base root
  where block_date >= timestamp '2022-08-16'
),

nxmty_supply_changes as (
  select
    date_trunc('day', evt_block_time) as block_date,
    sum(
      case when "from" = 0x0000000000000000000000000000000000000000 then 1 else -1 end
      * cast(value as double) / 1e18
    ) as supply_change
  from nexusmutual_ethereum.nexusmutualtreasuryyield_evt_transfer
  where "from" = 0x0000000000000000000000000000000000000000 -- mints
    or "to" = 0x0000000000000000000000000000000000000000 -- burns
  group by 1
),

vault_token_changes as (
  select
    date_trunc('day', evt_block_time) as block_date,
    contract_address,
    sum(
      case when "to" = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd then 1 else -1 end
      * cast(value as double) / 1e18
    ) as amount
  from erc20_ethereum.evt_transfer
  where evt_block_time >= timestamp '2022-08-16'
    and contract_address in (
      0xbeef050ecd6a16c4e7bffbb52ebba7846c4b8cd4, -- steakETH
      0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee, -- weETH
      0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2  -- WETH
    )
    and (
      "to" = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd
      or "from" = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd
    )
  group by 1, 2
  union all
  -- WETH9 wraps / unwraps emit Deposit / Withdrawal, not Transfer
  select
    date_trunc('day', evt_block_time) as block_date,
    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address,
    sum(cast(wad as double) / 1e18) as amount
  from zeroex_ethereum.weth9_evt_deposit
  where evt_block_time >= timestamp '2022-08-16'
    and dst = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd
  group by 1
  union all
  select
    date_trunc('day', evt_block_time) as block_date,
    0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as contract_address,
    -1 * sum(cast(wad as double) / 1e18) as amount
  from zeroex_ethereum.weth9_evt_withdrawal
  where evt_block_time >= timestamp '2022-08-16'
    and src = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd
  group by 1
),

vault_token_changes_agg as (
  select
    block_date,
    sum(case when contract_address = 0xbeef050ecd6a16c4e7bffbb52ebba7846c4b8cd4 then amount end) as steaketh_change,
    sum(case when contract_address = 0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee then amount end) as weeth_change,
    sum(case when contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 then amount end) as weth_change
  from vault_token_changes
  group by 1
),

steaketh_price_events as (
  select
    evt_block_time,
    cast(assets as double) / cast(shares as double) as price
  from metamorpho_vaults_ethereum.metamorpho_evt_deposit
  where contract_address = 0xbeef050ecd6a16c4e7bffbb52ebba7846c4b8cd4
    and evt_block_time >= timestamp '2025-12-01'
    and shares >= uint256 '1000000000000' -- exclude dust (rounding-distorted ratios)
  union all
  select
    evt_block_time,
    cast(assets as double) / cast(shares as double) as price
  from metamorpho_vaults_ethereum.metamorpho_evt_withdraw
  where contract_address = 0xbeef050ecd6a16c4e7bffbb52ebba7846c4b8cd4
    and evt_block_time >= timestamp '2025-12-01'
    and shares >= uint256 '1000000000000' -- exclude dust (rounding-distorted ratios)
),

steaketh_price_daily as (
  select
    date_trunc('day', evt_block_time) as block_date,
    min(evt_block_time) as first_time,
    min_by(price, evt_block_time) as first_price,
    max(evt_block_time) as last_time,
    max_by(price, evt_block_time) as last_price
  from steaketh_price_events
  group by 1
),

daily_filled as (
  select
    b.block_date,
    b.pool_nxmty_units,
    b.nxmty_eth_total,
    b.nxmty_price_eth,
    sum(coalesce(s.supply_change, 0)) over (order by b.block_date) as nxmty_total_supply,
    sum(coalesce(v.steaketh_change, 0)) over (order by b.block_date) as steaketh_units,
    sum(coalesce(v.weeth_change, 0)) over (order by b.block_date) as weeth_units,
    sum(coalesce(v.weth_change, 0)) over (order by b.block_date) as weth_units,
    -- last steakETH event at or before end of day, first event after it
    last_value(p.last_time) ignore nulls over (order by b.block_date rows between unbounded preceding and current row) as price_before_time,
    last_value(p.last_price) ignore nulls over (order by b.block_date rows between unbounded preceding and current row) as price_before,
    first_value(p.first_time) ignore nulls over (order by b.block_date rows between 1 following and unbounded following) as price_after_time,
    first_value(p.first_price) ignore nulls over (order by b.block_date rows between 1 following and unbounded following) as price_after
  from base b
    left join nxmty_supply_changes s on b.block_date = s.block_date
    left join vault_token_changes_agg v on b.block_date = v.block_date
    left join steaketh_price_daily p on b.block_date = p.block_date
),

daily_enriched as (
  select
    block_date,
    nxmty_price_eth,
    nxmty_total_supply,
    pool_nxmty_units,
    pool_nxmty_units / nullif(nxmty_total_supply, 0) as pool_nxmty_share,
    nullif(nxmty_eth_total, 0) as nxmty_eth_total, -- base drops to 0 after 3+ days without an oracle price
    steaketh_units,
    -- interpolate the steakETH share price to midnight (end of day) between the events either side
    coalesce(
      price_before + (price_after - price_before)
        * (to_unixtime(block_date + interval '1' day) - to_unixtime(price_before_time))
        / nullif(to_unixtime(price_after_time) - to_unixtime(price_before_time), 0),
      price_before
    ) as steaketh_price_eth,
    weth_units,
    weeth_units
  from daily_filled
),

daily_sleeves as (
  select
    *,
    steaketh_units * steaketh_price_eth * pool_nxmty_share as steaketh_eth_total,
    weth_units * pool_nxmty_share as weth_eth_total
  from daily_enriched
),

daily_lag as (
  select
    *,
    nxmty_eth_total - steaketh_eth_total - weth_eth_total as etherfi_eth_total,
    lag(nxmty_eth_total) over (order by block_date) as nxmty_eth_total_prev,
    lag(pool_nxmty_units) over (order by block_date) as pool_nxmty_units_prev,
    lag(pool_nxmty_share) over (order by block_date) as pool_nxmty_share_prev,
    lag(steaketh_units) over (order by block_date) as steaketh_units_prev,
    lag(steaketh_price_eth) over (order by block_date) as steaketh_price_eth_prev,
    lag(steaketh_eth_total) over (order by block_date) as steaketh_eth_total_prev
  from daily_sleeves
),

daily_returns as (
  select
    *,
    (pool_nxmty_units - pool_nxmty_units_prev) * nxmty_price_eth as pool_net_deposit_eth,
    nxmty_eth_total_prev * 0.0015 / 365 as enzyme_fee_eth, -- 0.14% management + 0.01% protocol, already deducted in the NXMTY oracle price
    nxmty_eth_total - nxmty_eth_total_prev
      - (pool_nxmty_units - pool_nxmty_units_prev) * nxmty_price_eth as nxmty_return_eth,
    pool_nxmty_share_prev * steaketh_units_prev * (steaketh_price_eth - steaketh_price_eth_prev)
      - steaketh_eth_total_prev * 0.0015 / 365 as steaketh_return_eth
  from daily_lag
)

select
  block_date,
  nxmty_price_eth,
  nxmty_total_supply,
  pool_nxmty_units,
  pool_nxmty_share,
  nxmty_eth_total,
  steaketh_units,
  steaketh_price_eth,
  steaketh_eth_total,
  weth_units,
  weth_eth_total,
  weeth_units,
  etherfi_eth_total,
  pool_net_deposit_eth,
  enzyme_fee_eth,
  nxmty_return_eth,
  steaketh_return_eth,
  nxmty_return_eth - steaketh_return_eth as etherfi_return_eth
from daily_returns
where block_date >= timestamp '{{Start Date}}'
  and block_date < timestamp '{{End Date}}'
order by block_date desc
