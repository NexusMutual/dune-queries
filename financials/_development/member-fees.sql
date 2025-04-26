with

membership as (
  -- v1
  select
    1 as version,
    date_trunc('day', call_block_time) as block_date,
    cardinality(userArray) as member_count
  from nexusmutual_ethereum.MemberRoles_call_addMembersBeforeLaunch
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  union all
  select
    1 as version,
    date_trunc('day', call_block_time) as block_date,
    count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_kycVerdict
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
    and verdict
  group by 2
  union all
  -- v2
  select
    2 as version,
    date_trunc('day', call_block_time) as block_date,
    count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_join
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  group by 2
  union all
  select
    2 as version,
    date_trunc('day', call_block_time) as block_date,
    -1 * count(*) as member_count
  from nexusmutual_ethereum.MemberRoles_call_withdrawMembership
  where contract_address = 0x055cc48f7968fd8640ef140610dd4038e1b03926
    and call_success
  group by 2
),

daily_avg_eth_prices as (
  select
    date_trunc('day', minute) as block_date,
    avg(price) as avg_eth_usd_price
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute >= timestamp '2019-05-23'
  group by 1
)

select
  date_trunc('month', m.block_date) as block_month,
  sum(m.member_count) as member_count,
  sum(m.member_count * 0.0020) as eth_member_fee,
  sum(m.member_count * 0.0020 * p.avg_eth_usd_price) as usd_member_fee
from membership m
  inner join daily_avg_eth_prices p on m.block_date = p.block_date
group by 1
order by 1 desc
