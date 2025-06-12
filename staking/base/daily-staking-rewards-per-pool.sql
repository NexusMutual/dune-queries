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
  --from query_4067736 -- staking rewards - base
  from nexusmutual_ethereum.staking_rewards
),

staking_pools as (
  select
    pool_id,
    min(block_date) as first_reward_date
  from staking_rewards
  group by 1
),

staking_pool_day_sequence as (
  select
    sp.pool_id,
    d.timestamp as block_date
  from utils.days d
    inner join staking_pools sp on d.timestamp >=sp.first_reward_date
)

select
  d.pool_id,
  d.block_date,
  sum(r.reward_amount_per_day) as reward_total,
  dense_rank() over (partition by d.pool_id order by d.block_date desc) as pool_date_rn
from staking_pool_day_sequence d
  left join staking_rewards r on d.pool_id = r.pool_id and d.block_date between r.cover_start_date_plus_one and r.cover_end_bucket_expiry_date
group by 1, 2
--order by 1, 2
