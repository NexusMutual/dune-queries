with

staking_rewards as (
  select
    pool_id,
    product_id,
    cover_id,
    cover_start_time,
    cover_end_bucket_expiry_time,
    reward_amount_expected_total,
    date_diff('second', cover_start_time, cover_end_bucket_expiry_time) as cover_period_seconds
  from query_4067736 -- staking rewards - base
  --from nexusmutual_ethereum.staking_rewards
),

first_days as (
  select pool_id, product_id, date_trunc('day', min(cover_start_time)) as first_reward_date
  from staking_rewards
  group by 1, 2
),

day_seq as (
  select f.pool_id, f.product_id, d.timestamp as block_date
  from utils.days d
    inner join first_days f on d.timestamp >= f.first_reward_date
),

daily as (
  select
    ds.pool_id,
    ds.product_id,
    ds.block_date,
    coalesce(sum(c.reward_contrib), 0) as reward_total,
    coalesce(sum(c.reward_contrib) filter (where c.cover_end_bucket_expiry_time >= now()), 0) as reward_total_on_active_cover
  from day_seq ds
    left join (
      select
        ds.pool_id,
        ds.product_id,
        ds.block_date,
        sr.cover_end_bucket_expiry_time,
        greatest(
          0,
          date_diff(
            'second',
            greatest(sr.cover_start_time, ds.block_date),
            least(sr.cover_end_bucket_expiry_time, date_add('day', 1, ds.block_date))
          )
        ) * (sr.reward_amount_expected_total / nullif(sr.cover_period_seconds, 0)) as reward_contrib
      from day_seq ds
        inner join staking_rewards sr
          on ds.pool_id = sr.pool_id
          and ds.product_id = sr.product_id
          and date_add('day', 1, ds.block_date) > sr.cover_start_time
          and ds.block_date < sr.cover_end_bucket_expiry_time
    ) c on ds.pool_id = c.pool_id and ds.product_id = c.product_id and ds.block_date = c.block_date
  group by 1, 2, 3
)

select
  pool_id,
  product_id,
  block_date,
  reward_total,
  reward_total_on_active_cover,
  dense_rank() over (partition by pool_id, product_id order by block_date desc) as pool_product_date_rn
from daily
--order by 1, 2
