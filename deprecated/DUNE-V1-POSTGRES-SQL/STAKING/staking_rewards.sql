WITH
  v1_product_info as (
    select
      "contract_address" as product_address,
      "syndicate" as syndicate,
      "name" as product_name,
      "type" as product_type
    from
      dune_user_generated.nexus_v1_product_info_view
  ),
  poolstaking_reward as (
    select
      DISTINCT date_trunc('day', "evt_block_time") as ts,
      "contractAddress" as product_address,
      "amount" * 1E-18 as reward_amount
    from
      nexusmutual."PooledStaking_evt_RewardAdded"
  ),
  reward_count as (
    select
      DISTINCT ts,
      product_address,
      SUM(reward_amount) OVER (PARTITION BY ts, product_address) as net_reward_amount
    from
      poolstaking_reward
  ),
  burn_reward_count as (
    select
      COALESCE(ts, date_trunc('day', "evt_block_time")) as day,
      COALESCE(
        reward_count.product_address,
        nexusmutual."PooledStaking_evt_Burned"."contractAddress"
      ) as product_address,
      COALESCE(net_reward_amount, 0) as net_reward_amount,
      COALESCE("amount" * -1E-18, 0) as sum_burnt
    from
      nexusmutual."PooledStaking_evt_Burned"
      FULL JOIN reward_count ON reward_count.product_address = nexusmutual."PooledStaking_evt_Burned"."contractAddress"
      AND reward_count.ts = date_trunc('day', "evt_block_time")
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
  SUM(sum_burnt) OVER (PARTITION BY product_name) as total_product_name_burnt,
  SUM(net_reward_amount) OVER (PARTITION BY syndicate) as total_syndicate_reward_amount,
  SUM(net_reward_amount) OVER (PARTITION BY product_name) as total_product_name_reward_amount,
  SUM(net_reward_amount) OVER (PARTITION BY product_type) as total_product_type_reward_amount
from
  burn_reward_count
  INNER JOIN v1_product_info ON v1_product_info.product_address = burn_reward_count.product_address
ORDER BY
  day DESC