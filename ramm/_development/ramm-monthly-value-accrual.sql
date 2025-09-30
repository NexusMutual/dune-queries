with

historical_prices as (
  select
    date_trunc('minute', minute) as block_minute,
    avg(price) as avg_eth_usd_price
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
    and minute > cast('2023-11-11' as timestamp)
  group by 1
),

latest_prices as (
  select
    minute as block_minute,
    symbol,
    price as usd_price
  from prices.usd_latest
  where symbol = 'ETH'
    and blockchain is null
    and contract_address is null
  order by 1 desc
  limit 1
),

calls as (
  select
    r.evt_tx_hash,
    max_by(c.output_0, c.call_trace_address) as pool_post_raw,
    max_by(s.output_0, s.call_trace_address) as supply_post_raw
  from nexusmutual_ethereum.ramm_evt_nxmswappedforeth r
    inner join nexusmutual_ethereum.pool_call_getpoolvalueineth c on r.evt_block_time = c.call_block_time and r.evt_tx_hash = c.call_tx_hash
    inner join nexusmutual_ethereum.nxmtoken_call_totalsupply s on r.evt_block_time = s.call_block_time and r.evt_tx_hash = s.call_tx_hash
  where c.call_success and s.call_success
  group by 1
),

raw_swaps as (
  select
    evt_tx_hash,
    evt_block_time as evt_time,
    evt_index,
    -1 * cast(nxmIn as double) as nxm_delta_raw,
    -1 * cast(ethOut as double) as eth_delta_raw
  from nexusmutual_ethereum.ramm_evt_nxmswappedforeth
  union all
  select
    evt_tx_hash,
    evt_block_time as evt_time,
    evt_index,
    cast(nxmOut as double) as nxm_delta_raw,
    cast(ethIn as double) as eth_delta_raw
  from nexusmutual_ethereum.ramm_evt_ethswappedfornxm
),

swaps as (
  select
    evt_tx_hash,
    evt_time,
    evt_index,
    nxm_delta_raw,
    eth_delta_raw,
    coalesce(sum(nxm_delta_raw) over (
      partition by evt_tx_hash
      order by evt_index
      rows between 1 following and unbounded following
    ), 0) as nxm_delta_after_raw,
    coalesce(sum(eth_delta_raw) over (
      partition by evt_tx_hash
      order by evt_index
      rows between 1 following and unbounded following
    ), 0) as eth_delta_after_raw
  from raw_swaps
),

states as (
  select
    s.evt_time,
    s.evt_index,
    date_trunc('minute', s.evt_time) as block_minute,
    (c.supply_post_raw - s.nxm_delta_after_raw) / 1e18 as s_post,
    (c.pool_post_raw - s.eth_delta_after_raw) / 1e18 as p_post,
    s.nxm_delta_raw / 1e18 as nxm_delta,
    s.eth_delta_raw / 1e18 as eth_delta
  from swaps s
    inner join calls c on s.evt_tx_hash = c.evt_tx_hash
),

bv_diff_nxm_eth_swap_ext as (
  select
    st.block_minute,
    -- helpers
    (st.p_post / st.s_post) as bv_per_nxm_post_eth,
    ((st.p_post - st.eth_delta) / (st.s_post - st.nxm_delta)) as bv_per_nxm_pre_eth,
    -- in ETH
    (st.p_post / st.s_post) - ((st.p_post - st.eth_delta) / (st.s_post - st.nxm_delta)) as bv_diff_per_nxm_in_eth,
    ( (st.p_post / st.s_post) - ((st.p_post - st.eth_delta) / (st.s_post - st.nxm_delta)) ) * st.s_post as bv_diff_eth,
    -- in NXM
    ( ( (st.p_post / st.s_post) - ((st.p_post - st.eth_delta) / (st.s_post - st.nxm_delta)) ) / (st.p_post / st.s_post) ) as bv_diff_per_nxm_in_nxm,
    ( ( (st.p_post / st.s_post) - ((st.p_post - st.eth_delta) / (st.s_post - st.nxm_delta)) ) * st.s_post ) / (st.p_post / st.s_post) as bv_diff_nxm
  from states st
),

bv_profit as (
  select
    s.block_minute,
    s.bv_diff_per_nxm_in_eth,
    s.bv_diff_eth,
    s.bv_diff_per_nxm_in_nxm,
    s.bv_diff_nxm,
    s.bv_diff_eth * p.avg_eth_usd_price as bv_diff_usd,
    s.bv_diff_eth * lp.usd_price as bv_diff_usd_latest
  from bv_diff_nxm_eth_swap_ext s
    inner join historical_prices p on s.block_minute = p.block_minute
    cross join latest_prices lp
),

monthly as (
  select
    date_trunc('month', block_minute) as block_month,
    sum(bv_diff_eth) as bv_diff_eth,
    sum(bv_diff_nxm) as bv_diff_nxm,
    sum(bv_diff_usd) as bv_diff_usd,
    sum(bv_diff_usd_latest) as bv_diff_usd_latest,
    sum(bv_diff_per_nxm_in_eth) as bv_diff_per_nxm_in_eth,
    sum(bv_diff_per_nxm_in_nxm) as bv_diff_per_nxm_in_nxm
  from bv_profit
  group by 1
),

monthly_incl_cummulative as (
  select
    block_month,
    bv_diff_eth,
    bv_diff_nxm,
    bv_diff_usd,
    bv_diff_usd_latest,
    bv_diff_per_nxm_in_eth,
    bv_diff_per_nxm_in_nxm,
    sum(bv_diff_eth) over (order by block_month) as bv_diff_eth_cummulative,
    sum(bv_diff_nxm) over (order by block_month) as bv_diff_nxm_cummulative,
    sum(bv_diff_usd) over (order by block_month) as bv_diff_usd_cummulative,
    sum(bv_diff_usd_latest) over (order by block_month) as bv_diff_usd_latest_cummulative
  from monthly
)

select
  block_month,
  bv_diff_eth,
  bv_diff_nxm,
  bv_diff_usd,
  bv_diff_per_nxm_in_eth,
  bv_diff_per_nxm_in_nxm,
  bv_diff_eth_cummulative,
  bv_diff_nxm_cummulative,
  bv_diff_usd_cummulative,
  bv_diff_usd_latest,
  bv_diff_usd_latest_cummulative,
  avg(bv_diff_eth) over (
    order by block_month
    rows between 2 preceding and current row
  ) as bv_diff_3m_moving_avg_eth,
  avg(bv_diff_nxm) over (
    order by block_month
    rows between 2 preceding and current row
  ) as bv_diff_3m_moving_avg_nxm,
  avg(bv_diff_usd) over (
    order by block_month
    rows between 2 preceding and current row
  ) as bv_diff_3m_moving_avg_usd
from monthly_incl_cummulative
where block_month >= timestamp '2024-01-01'
order by block_month
