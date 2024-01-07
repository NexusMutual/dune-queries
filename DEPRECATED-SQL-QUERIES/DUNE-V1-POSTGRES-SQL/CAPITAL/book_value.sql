WITH
  RECURSIVE eth_transactions as (
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
  lido_staking_net_steth as (
    select
      t_time as day,
      total as lido_ingress,
      0 as lido_egress -- This will require changing when we can withdraw ethereum
    from
      cte_name
    WHERE
      t_time is not null
  ),
  ethereum_price_average as (
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
  dai_eth_transactions as (
    select
      COALESCE(dai_transactions.day, eth.day) as day,
      COALESCE(dai_ingress, 0) as dai_ingress,
      COALESCE(dai_egress, 0) as dai_egress,
      COALESCE(eth_ingress, 0) as eth_ingress,
      COALESCE(eth_egress, 0) as eth_egress,
      COALESCE(dai_ingress - dai_egress, 0) as total_dai,
      COALESCE(net_eth, 0) as total_eth
    from
      dai_transactions
      FULL JOIN eth ON dai_transactions.day = eth.day
  ),
  all_transactions as (
    select
      COALESCE(
        dai_eth_transactions.day,
        lido_staking_net_steth.day
      ) as day,
      COALESCE(dai_ingress, 0) as dai_ingress,
      COALESCE(dai_egress, 0) as dai_egress,
      COALESCE(
        lido_ingress,
        (
          SELECT
            lido_ingress
          FROM
            lido_staking_net_steth as inner_table
          where
            COALESCE(
              dai_eth_transactions.day,
              lido_staking_net_steth.day
            ) > inner_table.day
            and inner_table.lido_ingress IS NOT NULL
          ORDER BY
            day desc
          LIMIT
            1
        ), 0
      ) as lido_ingress,
      lido_ingress as a,
      0 as lido_egress,
      COALESCE(eth_ingress, 0) as eth_ingress,
      COALESCE(eth_egress, 0) as eth_egress,
      COALESCE(total_dai, 0) as total_dai,
      total_eth
    from
      dai_eth_transactions
      FULL JOIN lido_staking_net_steth ON dai_eth_transactions.day = lido_staking_net_steth.day
  ),
  priced_all_transactions as (
    select
      distinct all_transactions.day,
      dai_ingress,
      dai_egress,
      lido_ingress,
      lido_egress,
      eth_ingress,
      eth_egress,
      total_dai,
      total_eth,
      lido_ingress - lido_egress as running_net_lido,
      SUM(total_dai) over (
        order by
          all_transactions.day ASC
      ) as running_net_dai,
      SUM(total_eth) over (
        order by
          all_transactions.day ASC
      ) as running_net_eth,
      eth_moving_average as moving_average_eth,
      1 as moving_average_dai
    from
      all_transactions
      left JOIN ethereum_price_ma7 ON all_transactions.day = ethereum_price_ma7.day
    ORDER BY
      all_transactions.day
  ),
  display_currency_total as (
    SELECT
      day,
      case
        when '{{display_currency}}' = 'USD' then (moving_average_dai * running_net_dai) + (
          moving_average_eth * (running_net_eth + running_net_lido)
        )
        when '{{display_currency}}' = 'ETH' then (
          (moving_average_dai * running_net_dai) / moving_average_eth
        ) + running_net_eth + running_net_lido
        ELSE -1
      END as running_total_display_curr
    FROM
      priced_all_transactions
  ),
  minted_nxm as (
    select
      date_trunc('day', evt_block_time) as day,
      sum(value / 1e18) as NXM_supply_minted
    from
      nexusmutual."NXMToken_evt_Transfer" t
    where
      t.from = '\x0000000000000000000000000000000000000000'
    GROUP by
      day
    ORDER BY
      day
  ),
  burnt_nxm as (
    select
      date_trunc('day', evt_block_time) as day,
      sum(-1 * value / 1e18) as NXM_supply_burnt
    from
      nexusmutual."NXMToken_evt_Transfer" t
    where
      t.to = '\x0000000000000000000000000000000000000000'
    GROUP by
      day
    ORDER BY
      day
  ),
  minted_burnt_nxm as (
    select
      CASE
        WHEN minted_nxm.day is NULL THEN burnt_nxm.day
        ELSE minted_nxm.day
      END as day,
      CASE
        WHEN NXM_supply_minted is NULL THEN 0
        ELSE NXM_supply_minted
      END as NXM_supply_minted,
      CASE
        WHEN NXM_supply_burnt is NULL THEN 0
        ELSE NXM_supply_burnt
      END as NXM_supply_burnt
    from
      minted_nxm
      FULL JOIN burnt_nxm ON minted_nxm.day = burnt_nxm.day
  ),
  total_nxm as (
    SELECT
      day,
      NXM_supply_minted + NXM_supply_burnt AS net_nxm,
      sum(NXM_supply_minted + NXM_supply_burnt) over (
        order by
          day asc rows between unbounded preceding
          and current row
      ) as total_nxm
    FROM
      minted_burnt_nxm
  )
select
  coalesce(display_currency_total.day, total_nxm.day) as day,
  running_total_display_curr,
  total_nxm,
  running_total_display_curr / total_nxm as book_value
FROM
  total_nxm
  INNER JOIN display_currency_total on display_currency_total.day = total_nxm.day
WHERE
  coalesce(display_currency_total.day, total_nxm.day) >= '{{1. Start Date}}'
  AND coalesce(display_currency_total.day, total_nxm.day) <= '{{2. End Date}}'
ORDER BY
  day DESC