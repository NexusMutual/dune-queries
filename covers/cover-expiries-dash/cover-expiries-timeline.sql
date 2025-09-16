with

active_covers as (
  select
    cover_id,
    cover_owner,
    cover_start_date,
    cover_end_date,
    cover_asset,
    sum_assured,
    --ETH
    eth_cover_amount,
    eth_usd_cover_amount,
    --DAI
    dai_eth_cover_amount,
    dai_usd_cover_amount,
    --USDC
    usdc_eth_cover_amount,
    usdc_usd_cover_amount,
    --cbBTC
    cbbtc_eth_cover_amount,
    cbbtc_usd_cover_amount
  from query_5785377 -- active covers - base root
  where cover_end_time <= case '{{expiry within}}'
      when '2 weeks' then current_date + interval '14' day
      when '1 month' then current_date + interval '1' month
      when '2 months' then current_date + interval '2' month
      when '3 months' then current_date + interval '3' month
      when '6 months' then current_date + interval '6' month
      when 'no end date' then (select cast(max(cover_end_time) as timestamp) from query_4599092)
    end
),

covers_agg as (
  select
    cover_id,
    cover_owner,
    cover_end_date,
    cover_asset,
    sum_assured as cover_amount,
    sum(eth_usd_cover_amount + dai_usd_cover_amount + usdc_usd_cover_amount + cbbtc_usd_cover_amount) as usd_cover_amount
  from active_covers
  group by 1, 2, 3, 4, 5
),

day_sequence as (
  select cast(d.seq_date as timestamp) as block_date
  from (
      select sequence(
        (select cast(min(cover_end_date) as timestamp) from covers_agg),
        (select cast(max(cover_end_date) as timestamp) from covers_agg),
        interval '1' day
      ) as days
    ) as days_s
    cross join unnest(days) as d(seq_date)
),

daily_cover_expiry as (
  select
    ds.block_date,
    sum(c.eth_cover_amount) as eth_eth_cover_total,
    sum(c.eth_usd_cover_amount) as eth_usd_cover_total,
    sum(c.dai_eth_cover_amount) as dai_eth_cover_total,
    sum(c.dai_usd_cover_amount) as dai_usd_cover_total,
    sum(c.usdc_eth_cover_amount) as usdc_eth_cover_total,
    sum(c.usdc_usd_cover_amount) as usdc_usd_cover_total,
    sum(c.cbbtc_eth_cover_amount) as cbbtc_eth_cover_total,
    sum(c.cbbtc_usd_cover_amount) as cbbtc_usd_cover_total
  from day_sequence ds
    left join active_covers c on ds.block_date between c.cover_start_date and c.cover_end_date
  group by 1
)

select
  block_date,
  -- just USD amounts
  sum(eth_usd_cover_total) as eth_cover_total,
  sum(dai_usd_cover_total) as dai_cover_total,
  sum(usdc_usd_cover_total) as usdc_cover_total,
  sum(cbbtc_usd_cover_total) as cbbtc_cover_total,
  sum(eth_usd_cover_total + dai_usd_cover_total + usdc_usd_cover_total + cbbtc_usd_cover_total) as cover_total
from daily_cover_expiry
group by 1
order by 1 desc
