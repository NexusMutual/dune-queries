with
  labels_dune as (
    SELECT
      DISTINCT "name",
      "address"
    FROM
      labels.labels
    WHERE
      "name" in ('dai', 'lido')
      and 'name' is not null
  ),
  erc_transactions as (
    select
      labels_dune.name as name,
      "contract_address",
      date_trunc('day', evt_block_time) as day,
      CASE
        WHEN "to" = '\xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN "to" = '\xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as ingress,
      CASE
        WHEN "from" = '\xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN "from" = '\xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as egress
    from
      erc20."ERC20_evt_Transfer"
      LEFT JOIN labels_dune ON labels_dune."address" = "ERC20_evt_Transfer"."contract_address"
    where
      value > 0
      AND NOT (
        (
          "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
          AND "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        )
        OR (
          "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        )
        OR (
          "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
          AND "from" = '\xfd61352232157815cf7b71045557192bf0ce1884'
        )
      )
      AND (
        "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR "to" = '\xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR "to" = '\xfD61352232157815cF7B71045557192Bf0CE1884' -- found from etherscan of mutant deploy contract
        OR "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR "from" = '\xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR "from" = '\xfD61352232157815cF7B71045557192Bf0CE1884'
      )
  ),
  lido_transactions as (
    SELECT
      DISTINCT day,
      SUM(ingress) OVER (PARTITION BY day) as lido_ingress,
      SUM(egress) OVER (PARTITION BY day) as lido_egress,
      SUM(ingress - egress) OVER (PARTITION BY day) as lido_net_total
    FROM
      erc_transactions
    WHERE
      "name" = 'lido'
  ),
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
      SUM(lido_net_total) OVER (
        ORDER BY
          lido.time ROWS BETWEEN UNBOUNDED PRECEDING
          AND 1 PRECEDING
      ) as staked_eth_nexus
    from
      lido
      FULL JOIN lido_transactions ON lido.time = lido_transactions.day
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