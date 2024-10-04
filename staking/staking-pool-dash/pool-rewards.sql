with

params as (
  select cast(split_part('{{pool}}', ' : ', 1) as int) as pool_id
),

active_covers as (
  select
    cover_id,
    cover_start_date,
    cover_end_date,
    staking_pool_id,
    product_id,
    product_type,
    product_name,
    premium_nxm,
    premium_nxm_eth,
    premium_nxm_usd
  from query_3834200 -- active covers base (fallback) query
  --from nexusmutual_ethereum.active_covers ac -- spell needs updating
  where cast(staking_pool_id as int) in (select pool_id from params)
),

current_rewards as (
  select
    product_id,
    sum(reward_total) as reward_total,
    min_by(reward_total, pool_product_date_rn) as latest_daily_reward_total -- min bc pool_date_rn is desc
  from query_4127951 -- daily staking rewards per product - base
  where cast(pool_id as int) in (select pool_id from params)
  group by 1
),

expected_rewards as (
  select
    product_id,
    sum(reward_expected_total) as reward_expected_total
  from query_4127963 -- expected staking rewards per product - base
  where cast(pool_id as int) in (select pool_id from params)
  group by 1
)

select
  coalesce(ac.product_name, 'Totals') as listing,
  sum(ac.premium_nxm) as premium_nxm,
  sum(cr.reward_total) as reward_total,
  sum(cr.latest_daily_reward_total) as daily_reward,
  sum(er.reward_expected_total) as expected_rewards
from active_covers ac
  left join current_rewards cr on ac.product_id = cr.product_id
  left join expected_rewards er on ac.product_id = er.product_id
group by grouping sets (
  (product_name), -- individual product totals
  ()              -- grand total for all products
)
order by case when product_name is null then 1 else 0 end, product_name
