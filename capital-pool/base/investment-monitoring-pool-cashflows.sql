with

params as (
  select timestamp '2025-10-01' as start_time -- current Pool era (migration from the previous Pool nets to zero)
),

pool_transfers as (
  select
    t.block_date,
    t.transfer_type,
    t.from_address,
    t.symbol,
    t.amount,
    t.tx_hash
  from query_4982562 t -- capital pool transfers - base
  where t.block_time >= (select start_time from params)
    and t.symbol in ('ETH', 'USDC', 'cbBTC')
),

cover_txs as (
  -- buys and edits; same tx set as covers v2 - base root (query_4599092), which is too heavy to join here
  select distinct evt_tx_hash as tx_hash
  from nexusmutual_ethereum.cover_evt_coverbought
  where evt_block_time >= (select start_time from params)
),

claim_payouts as (
  select
    evt_tx_hash as tx_hash,
    cast(amount as double) as payout_raw
  from nexusmutual_ethereum.claims_evt_claimpayoutredeemed
  where evt_block_time >= (select start_time from params)
  union all
  select
    call_tx_hash as tx_hash,
    cast(null as double) as payout_raw -- legacy path: all ETH out treated as claim
  from nexusmutual_ethereum.IndividualClaims_call_redeemClaimPayout
  where call_block_time >= (select start_time from params)
    and call_success
),

claim_txs as (
  select
    tx_hash,
    sum(payout_raw) as payout_raw -- null only when every row is legacy
  from claim_payouts
  group by 1
),

-- a claim tx pays the claim in the cover asset and refunds the claim deposit in ETH:
-- split its ETH out into the payout (ETH covers only) and the deposit refund
claim_tx_legs as (
  select
    t.tx_hash,
    max(t.block_date) as block_date,
    -1 * sum(if(t.symbol = 'ETH', t.amount, 0)) as eth_out,
    count_if(t.symbol <> 'ETH') > 0 as has_non_eth_payout
  from pool_transfers t
    inner join claim_txs ct on t.tx_hash = ct.tx_hash
  where t.transfer_type = 'out'
  group by 1
),

claim_tx_eth_split as (
  select
    l.block_date,
    case
      when l.has_non_eth_payout then 0
      when ct.payout_raw is null then l.eth_out
      else least(l.eth_out, ct.payout_raw / 1e18)
    end as claim_eth_out,
    l.eth_out
  from claim_tx_legs l
    inner join claim_txs ct on l.tx_hash = ct.tx_hash
),

classified as (
  select
    t.block_date,
    t.symbol,
    t.amount,
    case
      when t.transfer_type = 'in'
        and t.symbol = 'USDC'
        and t.from_address in (
          0xfc64382c9ce89ba1c21692a68000366a35ff0336, -- NM Foundation hot wallet
          0x963df0066ff8345922df88eebeb1095be4e4e12e  -- NM Foundation Safe
        ) then 'claim_recovery'
      when ct.tx_hash is not null and t.transfer_type = 'out' and t.symbol <> 'ETH' then 'claim'
      when ct.tx_hash is not null and t.transfer_type = 'out' and t.symbol = 'ETH' then 'claim_eth_split' -- replaced by claim_tx_eth_split
      when cv.tx_hash is not null then 'premium'
      when t.transfer_type = 'in'
        and t.symbol = 'ETH'
        and t.from_address = 0xcafeac11196a5cc352938aeed545b32d5b9646fa -- Claims: claim submission deposit
        then 'claim_deposit'
      when t.transfer_type = 'in'
        and t.symbol = 'ETH'
        and t.from_address in ( -- MemberRoles forwards the joining fee
          0xcafea2c575550512582090aa06d0a069e7236b9e, -- MemberRoles (current, from Nov 2025)
          0x055cc48f7968fd8640ef140610dd4038e1b03926  -- MemberRoles (previous, to Oct 2025)
        )
        and t.amount = 0.002 -- excludes the old MemberRoles' 6.594 ETH sweep at the Oct 2025 upgrade
        then 'member_fee'
      else 'other'
    end as category
  from pool_transfers t
    left join claim_txs ct on t.tx_hash = ct.tx_hash
    left join cover_txs cv on t.tx_hash = cv.tx_hash
),

classified_split as (
  select
    block_date,
    symbol,
    amount,
    category
  from classified
  where category <> 'claim_eth_split'
  union all
  select
    block_date,
    'ETH' as symbol,
    -1 * claim_eth_out as amount,
    'claim' as category
  from claim_tx_eth_split
  union all
  select
    block_date,
    'ETH' as symbol,
    -1 * (eth_out - claim_eth_out) as amount,
    'claim_deposit' as category
  from claim_tx_eth_split
),

daily_flows as (
  select
    block_date,
    sum(if(category = 'premium' and symbol = 'ETH', amount, 0)) as premium_eth,
    sum(if(category = 'premium' and symbol = 'USDC', amount, 0)) as premium_usdc,
    sum(if(category = 'premium' and symbol = 'cbBTC', amount, 0)) as premium_cbbtc,
    sum(if(category = 'claim' and symbol = 'ETH', amount, 0)) as claim_eth,
    sum(if(category = 'claim' and symbol = 'USDC', amount, 0)) as claim_usdc,
    sum(if(category = 'claim' and symbol = 'cbBTC', amount, 0)) as claim_cbbtc,
    sum(if(category = 'claim_recovery', amount, 0)) as claim_recovery_usdc,
    sum(if(category = 'claim_deposit', amount, 0)) as claim_deposit_eth,
    sum(if(category = 'member_fee', amount, 0)) as member_fee_eth,
    sum(if(category = 'other' and symbol = 'ETH', amount, 0)) as other_eth,
    sum(if(category = 'other' and symbol = 'USDC', amount, 0)) as other_usdc,
    sum(if(category = 'other' and symbol = 'cbBTC', amount, 0)) as other_cbbtc
  from classified_split
  group by 1
),

capital_pool as (
  select
    block_date,
    avg_eth_usd_price,
    avg_usdc_usd_price,
    avg_cbbtc_usd_price,
    avg_usdc_usd_price / avg_eth_usd_price as usdc_eth_rate,
    avg_cbbtc_usd_price / avg_eth_usd_price as cbbtc_eth_rate,
    lag(avg_usdc_usd_price / avg_eth_usd_price, 1) over (order by block_date) as usdc_eth_rate_prev,
    lag(avg_cbbtc_usd_price / avg_eth_usd_price, 1) over (order by block_date) as cbbtc_eth_rate_prev,
    lag(usdc_total, 1) over (order by block_date) as usdc_total_prev,
    lag(cover_re_usdc_total, 1) over (order by block_date) as cover_re_usdc_total_prev,
    lag(cbbtc_total, 1) over (order by block_date) as cbbtc_total_prev
  from query_4627588 -- Capital Pool - base root
)

select
  cp.block_date,
  coalesce(f.premium_eth, 0) as premium_eth,
  coalesce(f.premium_usdc, 0) as premium_usdc,
  coalesce(f.premium_cbbtc, 0) as premium_cbbtc,
  coalesce(f.claim_eth, 0) as claim_eth,
  coalesce(f.claim_usdc, 0) as claim_usdc,
  coalesce(f.claim_cbbtc, 0) as claim_cbbtc,
  coalesce(f.claim_recovery_usdc, 0) as claim_recovery_usdc,
  coalesce(f.claim_deposit_eth, 0) as claim_deposit_eth,
  coalesce(f.member_fee_eth, 0) as member_fee_eth,
  coalesce(f.other_eth, 0) as other_eth,
  coalesce(f.other_usdc, 0) as other_usdc,
  coalesce(f.other_cbbtc, 0) as other_cbbtc,
  cp.avg_eth_usd_price,
  cp.avg_usdc_usd_price,
  cp.avg_cbbtc_usd_price,
  coalesce(f.premium_usdc, 0) * cp.usdc_eth_rate as premium_usdc_eth,
  coalesce(f.premium_cbbtc, 0) * cp.cbbtc_eth_rate as premium_cbbtc_eth,
  coalesce(f.claim_usdc, 0) * cp.usdc_eth_rate as claim_usdc_eth,
  coalesce(f.claim_cbbtc, 0) * cp.cbbtc_eth_rate as claim_cbbtc_eth,
  coalesce(f.claim_recovery_usdc, 0) * cp.usdc_eth_rate as claim_recovery_usdc_eth,
  cp.usdc_total_prev * (cp.usdc_eth_rate - cp.usdc_eth_rate_prev) as fx_usdc_eth,
  cp.cover_re_usdc_total_prev * (cp.usdc_eth_rate - cp.usdc_eth_rate_prev) as fx_cover_re_eth,
  cp.cbbtc_total_prev * (cp.cbbtc_eth_rate - cp.cbbtc_eth_rate_prev) as fx_cbbtc_eth
from capital_pool cp
  left join daily_flows f on cp.block_date = f.block_date
where cp.block_date >= (select start_time from params)
order by 1 desc
