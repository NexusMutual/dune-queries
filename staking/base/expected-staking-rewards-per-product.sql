with

staking_rewards as (
  select
    block_time,
    block_date,
    pool_id,
    product_id,
    cover_id,
    cover_start_date,
    cover_end_date,
    reward_amount_expected_total,
    reward_amount_per_day,
    tx_hash
  --from query_4067736 -- staking rewards base
  from nexusmutual_ethereum.staking_rewards
)

select
  pool_id,
  product_id,
  sum(reward_amount_expected_total) as reward_expected_total
from staking_rewards
group by 1, 2
--order by 1
