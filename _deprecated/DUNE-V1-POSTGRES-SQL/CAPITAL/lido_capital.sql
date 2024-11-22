WITH
  RECURSIVE labels_dune as (
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
  lido_rewards_incremental as (
    select
      DATE_TRUNC('day', "evt_block_time") as time,
      "postTotalPooledEther" / "preTotalPooledEther" as increment
    from
      lido."LidoOracle_evt_PostTotalShares"
  ),
  lido_rewards as (
    select
      ROW_NUMBER() OVER () as row_,
      lido_rewards_incremental.time,
      COALESCE(lido_transactions.lido_net_total, 0) as lido_net_total,
      increment,
      SUM(COALESCE(lido_transactions.lido_net_total, 0)) OVER (
        ORDER BY
          lido_rewards_incremental.time ROWS BETWEEN UNBOUNDED PRECEDING
          AND 1 PRECEDING
      ) as staked_eth_nexus
    from
      lido_rewards_incremental
      FULL JOIN lido_transactions ON lido_rewards_incremental.time = lido_transactions.day
    ORDER BY
      lido_rewards_incremental.time
  ),
  cte_name AS (
    select
      row_,
      row_ + 1 as anchor,
      time as start_date,
      time as t_time,
      lag(time) OVER (
        order by
          time
      ) as last_it,
      increment,
      COALESCE(lido_net_total, 0) as total,
      0.0 as prev_total
    from
      lido_rewards
    where
      row_ < 2
    UNION ALL
    select
      t.row_,
      anchor + 1,
      start_date,
      t.time,
      lag(t_time) OVER (
        ORDER BY
          c.row_
      ) as last_it,
      t.increment,
      (c.total * t.increment) + t.lido_net_total,
      c.total as prev_total
    from
      cte_name as c
      LEFT JOIN lido_rewards t ON (t.row_ = c.anchor - 1)
    WHERE
      c.row_ is NOT NULL
  ),
  lido_staking_net_steth as (
    select
      t_time as day,
      total as lido_ingress,
      0 as lido_egress -- This will require changing when we can withdraw ethereum
    from
      cte_name
    WHERE
      t_time is not null
  )
SELECT
  *
FROM
  lido_staking_net_steth