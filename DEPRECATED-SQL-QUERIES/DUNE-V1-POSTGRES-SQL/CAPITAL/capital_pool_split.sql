WITH
  RECURSIVE ethereum_price_average as (
    select
      date_trunc('day', minute) as day,
      avg(price) as eth_avg_price
    from
      prices."layer1_usd"
    where
      symbol = 'ETH'
    GROUP BY
      day
    ORDER BY
      day
  ),
  ethereum_price_ma7 as (
    select
      day,
      eth_avg_price,
      avg(eth_avg_price) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as eth_moving_average
    from
      ethereum_price_average
    ORDER BY
      day DESC
  ),
  eth_transactions as (
    select
      "to" as to_,
      "from" as from_,
      date_trunc('day', block_time) as day,
      gas_used * 1E-18 as gas_used,
      CASE
        WHEN "to" = '\xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN "to" = '\xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as eth_ingress,
      CASE
        WHEN "from" = '\xfD61352232157815cF7B71045557192Bf0CE1884' THEN value * 1E-18
        WHEN "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8' THEN value * 1E-18
        WHEN "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b' THEN value * 1E-18
        WHEN "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb' THEN value * 1E-18
        WHEN "from" = '\xcafea8321b5109d22c53ac019d7a449c947701fb' THEN value * 1E-18
        ELSE 0
      END as eth_egress
    from
      ethereum."traces"
    where
      success = true
      and value > 0
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
      and (
        "to" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR "to" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR "to" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR "to" = '\xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR "to" = '\xfD61352232157815cF7B71045557192Bf0CE1884' -- found from etherscan of mutant deploy contract
        OR "from" = '\xcafea7934490ef8b9d2572eaefeb9d48162ea5d8'
        OR "from" = '\xcafeada4d15bbc7592113d5d5af631b5dcd53dcb'
        OR "from" = '\xcafea35ce5a2fc4ced4464da4349f81a122fd12b'
        OR "from" = '\xcafea8321b5109d22c53ac019d7a449c947701fb'
        OR "from" = '\xfD61352232157815cF7B71045557192Bf0CE1884' -- found from etherscan of mutant deploy contract
      )
    ORDER BY
      day
  ),
  eth as (
    select
      distinct day,
      'ETH' as pool,
      SUM(eth_ingress) OVER (PARTITION BY day) as eth_ingress,
      SUM(eth_egress) OVER (PARTITION BY day) as eth_egress,
      SUM(eth_ingress - eth_egress) OVER (PARTITION BY day) as net_eth
    from
      eth_transactions
  ),
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
  dai_transactions as (
    SELECT
      DISTINCT day,
      'DAI' as pool,
      SUM(ingress) OVER (PARTITION BY day) as dai_ingress,
      SUM(egress) OVER (PARTITION BY day) as dai_egress,
      SUM(ingress - egress) OVER (PARTITION BY day) as dai_net_total
    FROM
      erc_transactions
    WHERE
      "name" = 'dai'
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
  --
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
      ROW_NUMBER() OVER () as row_,
      lido.time,
      lido.rewards,
      lido.lido_staked,
      COALESCE(lido_net_total, 0) as lido_net_total,
      lido.rewards / lido.lido_staked as reward_per_eth,
      SUM(COALESCE(lido_net_total, 0)) OVER (
        ORDER BY
          lido.time ROWS BETWEEN UNBOUNDED PRECEDING
          AND 1 PRECEDING
      ) as staked_eth_nexus
    from
      lido
      FULL JOIN lido_transactions ON lido.time = lido_transactions.day
    ORDER BY
      lido.time
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
      reward_per_eth,
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
      t.reward_per_eth,
      (c.total * (t.reward_per_eth + 1)) + t.lido_net_total,
      c.total as prev_total
    from
      cte_name as c
      LEFT JOIN lido_rewards t ON (t.row_ = c.anchor - 1)
    WHERE
      c.row_ is NOT NULL
  ),
  lido_pool as (
    select
      'stETH' as pool,
      total as total,
      eth_moving_average
    from
      cte_name
      INNER JOIN ethereum_price_ma7 ON ethereum_price_ma7.day = t_time
    WHERE
      t_time is not null
      AND t_time >= '{{1. Start Date}}'
      AND t_time <= '{{2. End Date}}'
    ORDER BY
      t_time DESC
    limit
      1
  ), eth_pool as (
    select
      eth.day,
      'ETH' as pool,
      SUM(net_eth) over (
        order by
          eth.day
      ) as total,
      eth_moving_average
    from
      eth
      INNER JOIN ethereum_price_ma7 ON ethereum_price_ma7.day = eth.day
    where
      eth.day >= '{{1. Start Date}}'
      AND eth.day <= '{{2. End Date}}'
    ORDER BY
      day desc
    limit
      1
  ), dai_pool as (
    select
      'DAI' as pool,
      SUM(dai_ingress - dai_egress) over () as total,
      eth_moving_average
    from
      dai_transactions
      INNER JOIN ethereum_price_ma7 ON ethereum_price_ma7.day = dai_transactions.day
    where
      dai_transactions.day >= '{{1. Start Date}}'
      AND dai_transactions.day <= '{{2. End Date}}'
    ORDER BY
      dai_transactions.day desc
    limit
      1
  ), total_pools as (
    select
      pool,
      case
        when '{{display_currency}}' = 'USD' then total * eth_moving_average
        when '{{display_currency}}' = 'ETH' then total
        ELSE -1
      END as display_curr
    from
      lido_pool
    UNION
    select
      pool,
      case
        when '{{display_currency}}' = 'USD' then total
        when '{{display_currency}}' = 'ETH' then total / eth_moving_average
        ELSE -1
      END as display_curr
    from
      dai_pool
    UNION
    select
      pool,
      case
        when '{{display_currency}}' = 'USD' then total * eth_moving_average
        when '{{display_currency}}' = 'ETH' then total
        ELSE -1
      END as display_curr
    from
      eth_pool
  )
select
  *
from
  total_pools