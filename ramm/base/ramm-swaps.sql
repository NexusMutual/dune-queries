with

params as (
  select 0.01 as oracle_buffer
),

book_value as (
  select
    cp.block_date,
    cp.avg_eth_usd_price,
    cp.avg_capital_pool_usd_total / ns.total_nxm as usd_book_value,
    cp.avg_capital_pool_eth_total / ns.total_nxm as eth_book_value
  from query_4627588 cp -- Capital Pool - base root
    inner join query_4514857 ns -- NXM supply base
      on cp.block_date = ns.block_date
),

-- aggregate daily sells (NXM -> ETH) and their average below swap price
swaps_below as (
  select
    date_trunc('day', block_time) as block_date,
    sum(amount_in) as nxm_in_below,
    sum(amount_out) as eth_out_below,
    avg(swap_price) as nxm_eth_swap_price_below
  from query_4498669 -- RAMM swap events - base
  where swap_type = 'below'
  group by 1
),

-- aggregate daily buys (ETH -> NXM) and their average above swap price
swaps_above as (
  select
    date_trunc('day', block_time) as block_date,
    sum(amount_out) as nxm_out_above,
    sum(amount_in) as eth_in_above,
    avg(swap_price) as nxm_eth_swap_price_above
  from query_4498669 -- RAMM swap events - base
  where swap_type = 'above'
  group by 1
),

-- raw per-observation cumulative prices (above/below) for twap construction
obs_raw as (
  select
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    to_unixtime(evt_block_time) as ts,
    cast(priceCumulativeAbove as decimal(38,0)) as cum_above_raw,
    cast(priceCumulativeBelow as decimal(38,0)) as cum_below_raw
  from nexusmutual_ethereum.ramm_evt_observationupdated
),

-- previous cumulative per observation (to compute deltas)
obs_lag as (
  select
    block_time,
    block_date,
    ts,
    cum_above_raw,
    cum_below_raw,
    lag(cum_above_raw) over (order by block_time) as prev_above,
    lag(cum_below_raw) over (order by block_time) as prev_below
  from obs_raw
),

-- positive deltas only (ignore resets) to unwrap cumulatives
obs_deltas as (
  select
    block_time,
    block_date,
    ts,
    greatest(cast(cum_above_raw - coalesce(prev_above, cum_above_raw) as double), 0.0) as d_above,
    greatest(cast(cum_below_raw - coalesce(prev_below, cum_below_raw) as double), 0.0) as d_below
  from obs_lag
),

-- unwrapped cumulative series via running sums
obs_unwrapped as (
  select
    block_time,
    block_date,
    ts,
    sum(d_above) over (order by block_time rows between unbounded preceding and current row) as cum_above_unwrapped,
    sum(d_below) over (order by block_time rows between unbounded preceding and current row) as cum_below_unwrapped
  from obs_deltas
),

-- for each day: start/end unwrapped cumulatives and time bounds
obs_day_bounds as (
  select
    block_date,
    min_by(cum_above_unwrapped, block_time) as start_above,
    max_by(cum_above_unwrapped, block_time) as end_above,
    min_by(cum_below_unwrapped, block_time) as start_below,
    max_by(cum_below_unwrapped, block_time) as end_below,
    min(ts) as ts_start,
    max(ts) as ts_end
  from obs_unwrapped
  group by 1
),

-- raw daily twap numerators/denominators (scale to be calibrated)
obs_twap_raw as (
  select
    block_date,
    (end_above - start_above) / nullif(ts_end - ts_start, 0.0) as twap_raw_above,
    (end_below - start_below) / nullif(ts_end - ts_start, 0.0) as twap_raw_below
  from obs_day_bounds
),

-- learn a single global scale to align below twap to real below swaps
calib_below as (
  select
    approx_percentile(sb.nxm_eth_swap_price_below / ot.twap_raw_below, 0.5) as k_below
  from obs_twap_raw ot
    inner join swaps_below sb on ot.block_date = sb.block_date
  where ot.twap_raw_below > 0
),

-- scaled twap for below and scale-free ratio (above/below)
obs_twap as (
  select
    ot.block_date,
    ot.twap_raw_below * coalesce(k.k_below, 1.0) / 1e18 as nxm_eth_twap_price_below,
    ot.twap_raw_above / nullif(ot.twap_raw_below, 0.0) as twap_ratio_above_over_below
  from obs_twap_raw ot
    cross join calib_below k
),

-- combine per-day swap prices, twaps and ratio
day_vals as (
  select
    bv.block_date,
    sb.nxm_eth_swap_price_below,
    sa.nxm_eth_swap_price_above,
    ot.nxm_eth_twap_price_below,
    ot.twap_ratio_above_over_below
  from book_value bv
    left join swaps_below sb on bv.block_date = sb.block_date
    left join swaps_above sa on bv.block_date = sa.block_date
    left join obs_twap ot on bv.block_date = ot.block_date
),

-- indices of last day with a valid ratio and last day with a valid below price
ff_index as (
  select
    block_date,
    max(if(twap_ratio_above_over_below > 0, block_date, date '1970-01-01'))
      over (order by block_date rows between unbounded preceding and current row) as last_ratio_day,
    max(if(coalesce(nxm_eth_swap_price_below, nxm_eth_twap_price_below) is not null, block_date, date '1970-01-01'))
      over (order by block_date rows between unbounded preceding and current row) as last_below_day
  from day_vals
),

-- bring forward the last known ratio/below values
ff_vals as (
  select
    d.block_date,
    dv.nxm_eth_swap_price_below,
    dv.nxm_eth_swap_price_above,
    dv.nxm_eth_twap_price_below,
    dv.twap_ratio_above_over_below,
    dv_below_prev.nxm_eth_swap_price_below as prev_swap_below,
    dv_below_prev.nxm_eth_twap_price_below as prev_twap_below,
    dv_ratio_prev.twap_ratio_above_over_below as prev_ratio
  from day_vals dv
    inner join ff_index d on d.block_date = dv.block_date
    left join day_vals dv_below_prev on dv_below_prev.block_date = d.last_below_day
    left join day_vals dv_ratio_prev on dv_ratio_prev.block_date = d.last_ratio_day
),

-- fill helper
filled as (
  select
    f.block_date,
    -- below filled (swap -> twap -> carry-forward)
    coalesce(f.nxm_eth_swap_price_below, f.nxm_eth_twap_price_below, f.prev_swap_below, f.prev_twap_below) as below_filled,
    -- pure fallback for below when no same-day swap (twa p->carry)
    coalesce(f.nxm_eth_twap_price_below, f.prev_swap_below, f.prev_twap_below) as below_fallback,
    -- above candidate (swap -> ratio*below_filled)
    coalesce(
      f.nxm_eth_swap_price_above,
      case when coalesce(f.twap_ratio_above_over_below, f.prev_ratio) > 0
        then coalesce(f.nxm_eth_swap_price_below, f.nxm_eth_twap_price_below, f.prev_swap_below, f.prev_twap_below)
             * coalesce(f.twap_ratio_above_over_below, f.prev_ratio)
      else null end
    ) as above_candidate
  from ff_vals f
)

select
  bv.block_date,
  sb.nxm_in_below,
  sb.eth_out_below,

  -- below: preserve real swap spikes; only fill empty days (floor at BV * (1 - buffer))
  case
    when sb.nxm_eth_swap_price_below is not null
      then sb.nxm_eth_swap_price_below
    else greatest(f.below_fallback, bv.eth_book_value * (1.0 - p.oracle_buffer))
  end as nxm_eth_swap_price_below,

  case
    when sb.nxm_eth_swap_price_below is not null
      then sb.nxm_eth_swap_price_below * bv.avg_eth_usd_price
    else greatest(f.below_fallback, bv.eth_book_value * (1.0 - p.oracle_buffer)) * bv.avg_eth_usd_price
  end as nxm_usd_swap_price_below,

  sa.nxm_out_above,
  sa.eth_in_above,

  -- above: candidate or BV * (1 + buffer) floor
  case
    when f.above_candidate is null
      then bv.eth_book_value * (1.0 + p.oracle_buffer)
    when f.above_candidate < bv.eth_book_value * (1.0 + p.oracle_buffer)
      then bv.eth_book_value * (1.0 + p.oracle_buffer)
    else f.above_candidate
  end as nxm_eth_swap_price_above,

  case
    when f.above_candidate is null
      then bv.eth_book_value * (1.0 + p.oracle_buffer) * bv.avg_eth_usd_price
    when f.above_candidate < bv.eth_book_value * (1.0 + p.oracle_buffer)
      then bv.eth_book_value * (1.0 + p.oracle_buffer) * bv.avg_eth_usd_price
    else f.above_candidate * bv.avg_eth_usd_price
  end as nxm_usd_swap_price_above,

  bv.eth_book_value,
  bv.usd_book_value,
  bv.avg_eth_usd_price
from book_value bv
  inner join filled f on bv.block_date = f.block_date
  left join swaps_below sb on bv.block_date = sb.block_date
  left join swaps_above sa on bv.block_date = sa.block_date
  cross join params p
where bv.block_date >= cast('2023-11-28' as date)
