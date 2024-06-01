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
      date_trunc('day', "evt_block_time") as ts,
      "contractAddress" as product_address,
      "amount" * 1E-18 as reward_amount
    from
      nexusmutual."PooledStaking_evt_RewardAdded"
  ),
  culmalative_rewards as (
    select
      DISTINCT ts,
      poolstaking_reward.product_address,
      syndicate,
      product_name,
      product_type,
      SUM(reward_amount) OVER (
        ORDER BY
          ts
      ) as net_reward_amount,
      SUM(reward_amount) OVER (
        PARTITION BY syndicate
        ORDER BY
          ts
      ) as syndicate_reward_amount,
      SUM(reward_amount) OVER (
        PARTITION BY product_name
        ORDER BY
          ts
      ) as product_name_reward_amount,
      SUM(reward_amount) OVER (
        PARTITION BY product_type
        ORDER BY
          ts
      ) as product_type_reward_amount
    from
      poolstaking_reward
      INNER JOIN v1_product_info ON v1_product_info.product_address = poolstaking_reward.product_address
  ),
  staked as (
    select
      date_trunc('day', "evt_block_time") as ts,
      "staker" as staker,
      "contractAddress" as product_address,
      "amount" * 1E-18 as staked_amount,
      0 as unstaked_amount
    from
      nexusmutual."PooledStaking_evt_Staked"
  ),
  unstaked as (
    select
      date_trunc('day', "evt_block_time") as ts,
      "staker" as staker,
      "contractAddress" as product_address,
      0 as staked_amount,
      "amount" * 1E-18 as unstaked_amount
    from
      nexusmutual."PooledStaking_evt_Unstaked"
  ),
  net_staked as (
    select
      ts,
      product_address,
      staked_amount,
      unstaked_amount
    from
      staked
    union
    select
      ts,
      product_address,
      staked_amount,
      unstaked_amount
    from
      unstaked
  ),
  net_totals as (
    select
      DISTINCT ts,
      net_staked.product_address,
      syndicate,
      product_name,
      product_type,
      SUM(staked_amount) OVER (
        ORDER BY
          ts
      ) as net_staked_amount,
      SUM(unstaked_amount) OVER (
        ORDER BY
          ts
      ) as net_unstaked_amount,
      SUM(staked_amount) OVER (
        PARTITION BY syndicate
        ORDER BY
          ts
      ) as syndicate_staked_amount,
      SUM(unstaked_amount) OVER (
        PARTITION BY syndicate
        ORDER BY
          ts
      ) as syndicate_unstaked_amount,
      SUM(staked_amount) OVER (
        PARTITION BY product_name
        ORDER BY
          ts
      ) as product_name_staked_amount,
      SUM(unstaked_amount) OVER (
        PARTITION BY product_name
        ORDER BY
          ts
      ) as product_name_unstaked_amount,
      SUM(staked_amount) OVER (
        PARTITION BY product_type
        ORDER BY
          ts
      ) as product_type_staked_amount,
      SUM(unstaked_amount) OVER (
        PARTITION BY product_type
        ORDER BY
          ts
      ) as product_type_unstaked_amount
    from
      net_staked
      INNER JOIN v1_product_info ON v1_product_info.product_address = net_staked.product_address
  )
SELECT
  culmalative_rewards.ts,
  culmalative_rewards.product_address,
  culmalative_rewards.syndicate,
  culmalative_rewards.product_name,
  culmalative_rewards.product_type,
  net_staked_amount - net_unstaked_amount as running_total_nxm,
  syndicate_staked_amount - syndicate_unstaked_amount as running_syndicate_nxm,
  product_type_staked_amount - product_type_unstaked_amount as running_product_type_nxm,
  product_name_staked_amount - product_name_unstaked_amount as running_product_name_nxm,
  net_reward_amount,
  syndicate_reward_amount,
  product_name_reward_amount,
  product_type_reward_amount
FROM
  net_totals
  JOIN culmalative_rewards ON (
    culmalative_rewards.product_address = net_totals.product_address
  )
  AND (culmalative_rewards.ts = net_totals.ts)