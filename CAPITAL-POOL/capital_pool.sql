WITH
  eth_daily_transactions AS (
    SELECT
      day,
      eth_ingress,
      eth_egress,
      eth_ingress - eth_egress AS net_eth
    FROM
      nexusmutual_ethereum.capital_pool_eth_daily_transaction_summary
  ),
  labels AS (
    SELECT
      name,
      cASt(address AS varbinary) AS contract_address
    FROM
      labels.all
    WHERE
      name IN (
        'Maker: dai',
        'Lido: steth',
        'Rocketpool: RocketTokenRETH'
      )
  ),
  erc_transactions AS (
    SELECT
      name,
      CASt(a.contract_address AS varbinary) AS contract_address,
      DATE_TRUNC('day', evt_block_time) AS day,
      CASE
        WHEN "to" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627
        ) THEN CAST(value AS DOUBLE) * 1E-18
        ELSE 0
      END AS ingress,
      CASE
        WHEN "from" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627
        ) THEN CAST(value AS DOUBLE) * 1E-18
        ELSE 0
      END AS egress
    FROM
      erc20_ethereum.evt_Transfer AS a
      LEFT JOIN labels ON a.contract_address = labels.contract_address
    WHERE
      (
        "to" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627
        )
        OR "from" IN (
          0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8,
          0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb,
          0xcafea35ce5a2fc4ced4464da4349f81a122fd12b,
          0xcafea8321b5109d22c53ac019d7a449c947701fb,
          0xfd61352232157815cf7b71045557192bf0ce1884,
          0x7cbe5682be6b648cc1100c76d4f6c96997f753d6,
          0xcafea112Db32436c2390F5EC988f3aDB96870627
        )
      )
      AND evt_block_time > CAST('2019-01-01 00:00:00' AS TIMESTAMP)
      AND (
        name IN (
          'Maker: dai',
          'Lido: steth',
          'Rocketpool: RocketTokenRETH'
        )
        OR cast(a.contract_address as varbinary) = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd /* nxmty */
      )
      AND NOT (
        (
          "to" = 0xcafea35ce5a2fc4ced4464da4349f81a122fd12b
          AND "from" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
        )
        OR (
          "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
          AND "from" = 0xcafeada4d15bbc7592113d5d5af631b5dcd53dcb
        )
        OR (
          "to" = 0xcafea7934490ef8b9d2572eaefeb9d48162ea5d8
          AND "from" = 0xfd61352232157815cf7b71045557192bf0ce1884
        )
      )
  ),
  dai_transactions AS (
    SELECT DISTINCT
      day,
      SUM(ingress) OVER (
        PARTITION BY
          day
      ) AS dai_ingress,
      SUM(egress) OVER (
        PARTITION BY
          day
      ) AS dai_egress,
      SUM(ingress - egress) OVER (
        PARTITION BY
          day
      ) AS dai_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Maker: dai'
  ),
  rocket_pool_transactions AS (
    SELECT DISTINCT
      day,
      SUM(ingress - egress) OVER (
        PARTITION BY
          day
      ) AS rpl_net_total
    FROM
      erc_transactions
    WHERE
      name = 'Rocketpool: RocketTokenRETH'
  ),
  lido AS (
    SELECT
      1 AS anchor,
      DATE_TRUNC('day', evt_block_time) AS day,
      CAST(postTotalPooledEther AS DOUBLE) / CAST(totalShares AS DOUBLE) AS rebASe
    FROM
      lido_ethereum.LegacyOracle_evt_PostTotalShares
    WHERE
      evt_block_time > CAST('2021-05-26' AS TIMESTAMP)
  ),
  lido_staking_net_steth AS (
    SELECT DISTINCT
      1 AS anchor,
      lido.day AS day,
      ingress,
      egress,
      ingress - egress AS steth_amount,
      rebASe AS rebASe2
    FROM
      lido
      INNER JOIN erc_transactions ON erc_transactions.day = lido.day
      AND erc_transactions.name = 'Lido: steth'
  ),
  expanded_rebASe_steth AS (
    SELECT
      lido.day AS day,
      steth_amount,
      lido.rebASe AS rebASe,
      lido_staking_net_steth.rebASe2 AS rebASe2
    FROM
      lido_staking_net_steth
      FULL JOIN lido ON lido.anchor = lido_staking_net_steth.anchor
      AND lido_staking_net_steth.day <= lido.day
    ORDER BY
      lido.day DESC
  ),
  steth AS (
    SELECT DISTINCT
      day,
      SUM(
        steth_amount * CAST(rebASe AS DOUBLE) / CAST(rebASe2 AS DOUBLE)
      ) OVER (
        PARTITION BY
          day
      ) AS lido_ingress
    FROM
      expanded_rebASe_steth
  ),
  weth_nxmty_transactions AS (
    select distinct
      day,
      SUM(ingress) OVER (
        PARTITION BY
          day
      ),
      SUM(egress) OVER (
        PARTITION BY
          day
      ),
      SUM(ingress - egress) OVER (
        PARTITION BY
          day
      ) AS value
    from
      erc_transactions
    where
      erc_transactions.contract_address = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd
  ),
  chainlink_oracle_nxmty_price AS (
    SELECT
      date_trunc('day', evt_block_time) AS day,
      CAST(answer AS double) / 1e18 AS nxmty_price
    FROM
      chainlink_ethereum.AccessControlledOffchainAggregator_evt_NewTransmission
    WHERE
      contract_address = 0xca71bbe491079e138927f3f0ab448ae8782d1dca
      AND evt_block_time > CAST('2022-08-15 00:00:00' AS TIMESTAMP)
  ),
  nxmty AS (
    SELECT
      chainlink_oracle_nxmty_price.day,
      nxmty_price,
      COALESCE(value, 0) AS net_enzyme
    FROM
      chainlink_oracle_nxmty_price
      FULL JOIN weth_nxmty_transactions ON weth_nxmty_transactions.day = chainlink_oracle_nxmty_price.day
  ),
  day_prices AS (
    SELECT DISTINCT
      date_trunc('day', minute) AS day,
      symbol,
      AVG(price) OVER (
        PARTITION BY
          date_trunc('day', minute),
          symbol
      ) AS price_dollar
    FROM
      prices.usd
    WHERE
      (
        symbol = 'DAI'
        OR symbol = 'ETH'
        OR symbol = 'rETH'
      )
      AND minute > CAST('2019-05-23' AS TIMESTAMP)
  ),
  eth_day_prices AS (
    SELECT
      day,
      price_dollar as eth_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'ETH'
  ),
  dai_day_prices AS (
    SELECT
      day,
      price_dollar AS dai_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'DAI'
  ),
  rpl_day_prices AS (
    SELECT
      day,
      price_dollar AS rpl_price_dollar
    FROM
      day_prices
    WHERE
      symbol = 'rETH'
  ),
  ethereum_price_ma7 as (
    select
      day,
      eth_price_dollar,
      avg(eth_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) AS moving_average_eth
    from
      eth_day_prices
    ORDER BY
      day DESC
  ),
  dai_price_ma7 AS (
    select
      day,
      dai_price_dollar,
      avg(dai_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) as moving_average_dai
    from
      dai_day_prices
    ORDER BY
      day DESC
  ),
  rpl_price_ma7 AS (
    SELECT
      day,
      rpl_price_dollar,
      AVG(rpl_price_dollar) OVER (
        ORDER BY
          day ROWS BETWEEN 6 PRECEDING
          AND CURRENT ROW
      ) AS moving_average_rpl
    FROM
      rpl_day_prices
    ORDER BY
      day DESC
  ),
  price_ma as (
    select
      ethereum_price_ma7.day,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai,
      rpl_price_ma7.moving_average_rpl
    from
      ethereum_price_ma7
      LEFT JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
      LEFT JOIN rpl_price_ma7 ON ethereum_price_ma7.day = rpl_price_ma7.day
  ),
  all_running_totals as (
    select
      price_ma.day as day,
      moving_average_eth,
      moving_average_dai,
      moving_average_rpl,
      eth_ingress,
      eth_egress,
      SUM(COALESCE(net_eth, 0)) OVER (
        ORDER BY
          price_ma.day
      ) as running_net_eth,
      SUM(COALESCE(net_enzyme, 0)) OVER (
        ORDER BY
          price_ma.day
      ) * COALESCE(
        nxmty_price,
        LAG(nxmty_price) OVER (
          ORDER BY
            price_ma.day ASC
        ),
        0
      ) AS running_net_enzyme,
      SUM(COALESCE(dai_net_total, 0)) OVER (
        ORDER BY
          price_ma.day
      ) as running_net_dai,
      SUM(COALESCE(rpl_net_total, 0)) OVER (
        ORDER BY
          price_ma.day
      ) as running_net_rpl,
      COALESCE(
        lido_ingress,
        LAG(lido_ingress) OVER (
          ORDER BY
            price_ma.day
        ),
        0
      ) as running_net_lido
    from
      price_ma
      LEFT JOIN nxmty ON price_ma.day = nxmty.day
      LEFT JOIN dai_transactions ON price_ma.day = dai_transactions.day
      LEFT JOIN rocket_pool_transactions ON price_ma.day = rocket_pool_transactions.day
      LEFT JOIN steth ON price_ma.day = steth.day
      LEFT JOIN eth_daily_transactions ON price_ma.day = eth_daily_transactions.day
  )
SELECT
  day,
  moving_average_eth,
  moving_average_dai,
  case
    when '{{display_currency}}' = 'USD' then moving_average_eth * running_net_eth
    when '{{display_currency}}' = 'ETH' then running_net_eth
    ELSE -1
  END as running_net_eth_display_curr,
  case
    when '{{display_currency}}' = 'USD' then moving_average_dai * running_net_dai
    when '{{display_currency}}' = 'ETH' then moving_average_dai * running_net_dai / moving_average_eth
    ELSE -1
  END as running_net_dai_display_curr,
  case
    when '{{display_currency}}' = 'USD' then moving_average_rpl * running_net_rpl
    when '{{display_currency}}' = 'ETH' then moving_average_rpl * running_net_rpl / moving_average_eth
    ELSE -1
  END as running_net_rpl_display_curr,
  case
    when '{{display_currency}}' = 'USD' then moving_average_eth * running_net_lido
    when '{{display_currency}}' = 'ETH' then running_net_lido
    ELSE -1
  END as running_net_lido_display_curr,
  case
    when '{{display_currency}}' = 'USD' then moving_average_eth * running_net_enzyme
    when '{{display_currency}}' = 'ETH' then running_net_enzyme
    ELSE -1
  END as running_net_enzyme_display_curr,
  case
    when '{{display_currency}}' = 'USD' then (moving_average_dai * running_net_dai) + (moving_average_rpl * running_net_rpl) + (
      moving_average_eth * (
        running_net_eth + running_net_lido + running_net_enzyme
      )
    )
    when '{{display_currency}}' = 'ETH' then (
      (moving_average_dai * running_net_dai) / moving_average_eth
    ) + running_net_eth + running_net_lido + running_net_enzyme + (moving_average_rpl * running_net_rpl / moving_average_eth)
    ELSE -1
  END as running_total_display_curr
FROM
  all_running_totals
WHERE
  day >= CAST('{{Start Date}}' AS TIMESTAMP)
  AND day <= CAST('{{End Date}}' AS TIMESTAMP)
ORDER BY
  day DESC