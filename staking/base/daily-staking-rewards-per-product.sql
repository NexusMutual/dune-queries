with

staking_rewards as (
  select
    block_date,
    pool_id,
    product_id,
    cover_id,
    date_add('day', 1, cover_start_date) as cover_start_date_plus_one,
    cover_end_bucket_expiry_date,
    reward_amount_expected_total,
    reward_amount_per_day
  --from query_4067736 -- staking rewards base
  from nexusmutual_ethereum.staking_rewards
),

staking_pools as (
  select
    pool_id,
    product_id,
    min(block_date) as first_reward_date
  from staking_rewards
  group by 1, 2
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    sp.product_id,
    s.block_date
  from staking_pools sp
    cross join unnest (
      sequence(
        cast(date_trunc('day', sp.first_reward_date) as timestamp),
        cast(date_trunc('day', now()) as timestamp),
        interval '1' day
      )
    ) as s(block_date)
)

select
  d.pool_id,
  d.product_id,
  d.block_date,
  sum(r.reward_amount_per_day) as reward_total,
  sum(r.reward_amount_per_day) filter (where r.cover_end_bucket_expiry_date >= now()) as reward_total_on_active_cover,
  dense_rank() over (partition by d.pool_id, d.product_id order by d.block_date desc) as pool_product_date_rn
from staking_pool_day_sequence d
  left join staking_rewards r on d.pool_id = r.pool_id and d.product_id = r.product_id
    and d.block_date between r.cover_start_date_plus_one and r.cover_end_bucket_expiry_date
group by 1, 2, 3
--order by 1, 2
