with
  rewards as (
    select
      DATE_TRUNC('day', "evt_block_time") as time,
      sum("value") / 1e18 rewards
    from
      erc20."ERC20_evt_Transfer" t
    where
      t."from" = '\x0000000000000000000000000000000000000000'
      and t."contract_address" = '\xae7ab96520de3a18e5e111b5eaab095312d7fe84'
      and t."to" in (
        select
          "rewardAddress" address
        from
          lido."NodeOperatorsRegistry_evt_NodeOperatorAdded"
        union all
        select
          CAST(
            '\x3e40D73EB977Dc6a537aF587D48316feE66E9C8c' AS bytea
          )
      )
    group by
      1
  ),
  lido_amounts as (
    select
      DATE_TRUNC('day', "evt_block_time") as time,
      sum (sum("amount") / 1e18) over (
        order by
          DATE_TRUNC('day', "evt_block_time")
      ) lido_staked
    from
      lido."steth_evt_Submitted"
    group by
      1
  ),
  nexus_lido_staked as (
    select
      DATE_TRUNC('day', "evt_block_time") as time,
      "value" as value
    from
      erc20."ERC20_evt_Transfer" t
    where
      t."to" = '\xcafea35cE5a2fc4CED4464DA4349f81A122fd12b'
      and t."from" = '\x3e40D73EB977Dc6a537aF587D48316feE66E9C8c'
  ),
  lido as (
    select
      rewards.time,
      rewards,
      lido_amounts.lido_staked
    from
      rewards
      left join lido_amounts on rewards.time = lido_amounts.time
    order by
      1 desc
  ),
  lido_rewards as (
    select
      lido.time,
      lido.rewards,
      lido.lido_staked,
      SUM(value) OVER (
        ORDER BY
          lido.time ROWS BETWEEN UNBOUNDED PRECEDING
          AND 1 PRECEDING
      ) as staked_eth_nexus
    from
      lido
      FULL JOIN nexus_lido_staked ON lido.time = nexus_lido_staked.time
    ORDER BY
      lido.time
  )
select
  *,
  SUM((staked_eth_nexus * rewards / lido_staked)) OVER (
    ORDER BY
      time ROWS BETWEEN UNBOUNDED PRECEDING
      AND 1 PRECEDING
  ) as compound_reward,
  staked_eth_nexus + SUM((staked_eth_nexus * rewards / lido_staked)) OVER (
    ORDER BY
      time ROWS BETWEEN UNBOUNDED PRECEDING
      AND 1 PRECEDING
  )
from
  lido_rewards t
WHERE
  staked_eth_nexus > 0