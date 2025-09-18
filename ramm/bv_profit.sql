with

prices as (
  select
    date_trunc('minute', minute) as block_minute,
    avg(price) as avg_eth_usd_price
  from prices.usd
  where symbol = 'ETH'
    and blockchain is null
    and minute > cast('2023-11-11' as timestamp)
  group by 1
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

swaps as (
  select
    evt_tx_hash,
    evt_block_time as evt_time,
    evt_index,
    nxmIn,
    ethOut,
    coalesce(sum(nxmIn) over (
      partition by evt_tx_hash
      order by evt_index
      rows between 1 following and unbounded following
    ), 0) as nxm_in_after,
    coalesce(sum(ethOut) over (
      partition by evt_tx_hash
      order by evt_index
      rows between 1 following and unbounded following
    ), 0) as eth_out_after
  from nexusmutual_ethereum.ramm_evt_nxmswappedforeth
),

states as (
  select
    s.evt_tx_hash,
    s.evt_time,
    s.evt_index,
    date_trunc('minute', s.evt_time) as block_minute,
    (c.supply_post_raw / 1e18) + (s.nxm_in_after / 1e18) as s_post,
    (c.pool_post_raw / 1e18) + (s.eth_out_after / 1e18) as p_post,
    s.nxmIn / 1e18 as nxm_in,
    s.ethOut / 1e18 as eth_out
  from swaps s
    inner join calls c on s.evt_tx_hash = c.evt_tx_hash
),

bv_diff as (
  select
    st.block_minute,
    st.evt_time,
    st.evt_index,
    st.s_post,
    st.p_post,
    st.s_post + st.nxm_in as s_pre,
    st.p_post + st.eth_out as p_pre,
    (st.p_post / st.s_post) - ((st.p_post + st.eth_out) / (st.s_post + st.nxm_in)) as bv_diff_per_nxm,
    ((st.p_post / st.s_post) - ((st.p_post + st.eth_out) / (st.s_post + st.nxm_in))) * st.s_post as bv_diff
  from states st
),

bv_profit as (
  select
    d.block_minute,
    d.bv_diff_per_nxm,
    d.bv_diff,
    sum(d.bv_diff) over (
      order by d.evt_time, d.evt_index
      rows between unbounded preceding and current row
    ) as bv_diff_cummulative_eth,
    sum(d.bv_diff * p.avg_eth_usd_price) over (
      order by d.evt_time, d.evt_index
      rows between unbounded preceding and current row
    ) as bv_diff_cummulative_usd
  from bv_diff d
    inner join prices p on d.block_minute = p.block_minute
)

select
  block_minute,
  bv_diff_per_nxm,
  bv_diff,
  bv_diff_cummulative_eth,
  bv_diff_cummulative_usd,
  if('{{currency}}' = 'usd', bv_diff_cummulative_usd, bv_diff_cummulative_eth) as bv_diff_cummulative
from bv_profit
order by 1 desc
