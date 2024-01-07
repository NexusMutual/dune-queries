WITH
  v1_product_info as (
    SELECT
      product_contract_address,
      product_name,
      product_type,
      syndicate
    FROM
      nexusmutual_ethereum.product_information
  ),
  reward_count as (
    select DISTINCT
      date_trunc('day', evt_block_time) as ts,
      contractAddress as product_address,
      SUM(amount * -1E-18) OVER (
        PARTITION BY
          date_trunc('day', evt_block_time),
          contractAddress
      ) as net_reward_amount
    from
      nexusmutual_ethereum.PooledStaking_evt_RewardAdded
  ),
  burn_count as (
    select DISTINCT
      date_trunc('day', evt_block_time) as ts,
      contractAddress as product_address,
      SUM(amount * -1E-18) OVER (
        PARTITION BY
          date_trunc('day', evt_block_time),
          contractAddress
      ) as sum_burnt
    from
      nexusmutual_ethereum.PooledStaking_evt_Burned
  ),
  rewards_burns AS (
    SELECT
      COALESCE(reward_count.ts, burn_count.ts) as day,
      COALESCE(
        reward_count.product_address,
        burn_count.product_address
      ) as product_address,
      COALESCE(net_reward_amount, 0) as net_reward_amount,
      COALESCE(sum_burnt, 0) as sum_burnt
    FROM
      burn_count
      FULL JOIN reward_count ON reward_count.product_address = burn_count.product_address
      AND reward_count.ts = burn_count.ts
  )
select
  *,
  SUM(net_reward_amount) OVER (
    ORDER BY
      day
  ) as running_reward,
  SUM(sum_burnt) OVER (
    ORDER BY
      day
  ) as total_burns,
  SUM(net_reward_amount) OVER (
    ORDER BY
      day
  ) + SUM(sum_burnt) OVER (
    ORDER BY
      day
  ) as net_running_reward,
  SUM(sum_burnt) OVER (
    PARTITION BY
      product_name
  ) as total_product_name_burnt,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      syndicate
  ) as total_syndicate_reward_amount,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      product_name
  ) as total_product_name_reward_amount,
  SUM(net_reward_amount) OVER (
    PARTITION BY
      product_type
  ) as total_product_type_reward_amount
from
  rewards_burns
  LEFT JOIN v1_product_info ON v1_product_info.product_contract_address = rewards_burns.product_address
WHERE
  day >= '{{Start Date}}'
  AND day <= '{{End Date}}'
ORDER BY
  day DESC