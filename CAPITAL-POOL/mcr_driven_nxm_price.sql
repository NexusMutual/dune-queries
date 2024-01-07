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
      CAST(address AS varbinary) AS contract_address
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
      CAST(a.contract_address AS varbinary) AS contract_address,
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
        OR CAST(a.contract_address AS varbinary) = 0x27f23c710dd3d878fe9393d93465fed1302f2ebd /* nxmty */
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
      CAST(postTotalPooledEther AS DOUBLE) / CAST(totalShares AS DOUBLE) AS rebase
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
      rebase AS rebase2
    FROM
      lido
      INNER JOIN erc_transactions ON erc_transactions.day = lido.day
      AND erc_transactions.name = 'Lido: steth'
  ),
  expanded_rebase_steth AS (
    SELECT
      lido.day AS day,
      steth_amount,
      lido.rebase AS rebase,
      lido_staking_net_steth.rebase2 AS rebase2
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
        steth_amount * CAST(rebase AS DOUBLE) / CAST(rebase2 AS DOUBLE)
      ) OVER (
        PARTITION BY
          day
      ) AS lido_ingress
    FROM
      expanded_rebase_steth
  ),
  weth_nxmty_transactions AS (
    SELECT DISTINCT
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
      price_dollar AS eth_price_dollar
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
  price AS (
    SELECT
      a.day,
      eth_price_dollar,
      COALESCE(b.dai_price_dollar, 0) AS dai_price_dollar,
      COALESCE(c.rpl_price_dollar, 0) AS rpl_price_dollar
    FROM
      eth_day_prices AS a
      LEFT JOIN dai_day_prices AS b ON a.day = b.day
      LEFT JOIN rpl_day_prices AS c ON a.day = c.day
  ),
  MCR_event AS (
    select
      date_trunc('day', evt_block_time) as day,
      RANK() OVER (
        PARTITION BY
          date_trunc('day', evt_block_time)
        ORDER BY
          evt_block_time DESC
      ) as day_rank,
      mcrEtherx100 * 1E-18 as mcr_eth
    from
      nexusmutual_ethereum.MCR_evt_MCREvent
  ),
  MCR_updated as (
    select
      date_trunc('day', evt_block_time) as day,
      RANK() OVER (
        PARTITION BY
          date_trunc('day', evt_block_time)
        ORDER BY
          evt_block_time DESC
      ) as day_rank,
      mcr * 1E-18 as mcr_eth
    from
      nexusmutual_ethereum.MCR_evt_MCRUpdated
  ),
  MCR_updated_event as (
    select
      day,
      mcr_eth
    from
      MCR_event
    WHERE
      day_rank = 1
    UNION ALL
    select
      day,
      mcr_eth
    from
      MCR_updated
    WHERE
      day_rank = 1
  ),
  all_running_totals as (
    select DISTINCT
      a.day as day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      eth_ingress,
      eth_egress,
      net_eth,
      net_enzyme,
      nxmty_price,
      dai_net_total,
      rpl_net_total,
      lido_ingress,
      mcr_eth
    from
      price as a
      LEFT JOIN nxmty ON a.day = nxmty.day
      LEFT JOIN dai_transactions ON a.day = dai_transactions.day
      LEFT JOIN rocket_pool_transactions ON a.day = rocket_pool_transactions.day
      LEFT JOIN steth ON a.day = steth.day
      LEFT JOIN eth_daily_transactions ON a.day = eth_daily_transactions.day
      LEFT JOIN MCR_updated_event ON a.day = MCR_updated_event.day
  ),
  min_count_lagged AS (
    select
      day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      net_eth,
      dai_net_total,
      rpl_net_total,
      net_enzyme,
      nxmty_price,
      lido_ingress,
      mcr_eth,
      COUNT(net_eth) OVER (
        ORDER BY
          day
      ) AS running_net_eth_min_count,
      COUNT(dai_net_total) OVER (
        ORDER BY
          day
      ) AS dai_net_total_min_count,
      COUNT(net_enzyme) OVER (
        ORDER BY
          day
      ) AS net_enzyme_min_count,
      COUNT(nxmty_price) OVER (
        ORDER BY
          day
      ) AS nxmty_price_min_count,
      COUNT(lido_ingress) OVER (
        ORDER BY
          day
      ) AS lido_ingress_min_count,
      COUNT(mcr_eth) OVER (
        ORDER BY
          day
      ) AS mcr_eth_min_count
    from
      all_running_totals
  ),
  lagged AS (
    select
      day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      COALESCE(
        FIRST_VALUE(net_eth) OVER (
          PARTITION BY
            running_net_eth_min_count
          ORDER BY
            day
        ),
        0
      ) AS net_eth,
      COALESCE(
        FIRST_VALUE(dai_net_total) OVER (
          PARTITION BY
            dai_net_total_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_dai,
      COALESCE(rpl_net_total, 0) AS running_net_rpl,
      COALESCE(
        FIRST_VALUE(net_enzyme) OVER (
          PARTITION BY
            net_enzyme_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_enzyme,
      COALESCE(
        FIRST_VALUE(nxmty_price) OVER (
          PARTITION BY
            nxmty_price_min_count
          ORDER BY
            day
        ),
        0
      ) AS nxmty_price,
      COALESCE(
        FIRST_VALUE(lido_ingress) OVER (
          PARTITION BY
            lido_ingress_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_lido,
      FIRST_VALUE(mcr_eth) OVER (
        PARTITION BY
          mcr_eth_min_count
        ORDER BY
          day
      ) AS mcr_eth
    from
      min_count_lagged
  ),
  summed AS (
    SELECT
      day,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      SUM(net_eth) OVER (
        ORDER BY
          day
      ) as running_net_eth,
      SUM(running_net_dai) OVER (
        ORDER BY
          day
      ) AS running_net_dai,
      SUM(running_net_rpl) OVER (
        ORDER BY
          day
      ) AS running_net_rpl,
      SUM(running_net_enzyme * nxmty_price) OVER (
        ORDER BY
          day
      ) AS running_net_enzyme,
      running_net_lido,
      COALESCE(mcr_eth, 0) as mcr_eth
    FROM
      lagged
  ),
  nxm_token_price as (
    SELECT
      day,
      mcr_eth,
      eth_price_dollar,
      dai_price_dollar,
      rpl_price_dollar,
      running_net_eth,
      running_net_enzyme,
      running_net_lido,
      running_net_rpl,
      running_net_dai / eth_price_dollar as running_net_dai,
      (
        running_net_eth + running_net_enzyme + running_net_lido + (running_net_dai / eth_price_dollar) + (
          running_net_rpl * rpl_price_dollar / eth_price_dollar
        )
      ) as total,
      CASE
        WHEN mcr_eth = 0 THEN 0
        ELSE CAST(
          CAST(0.01028 AS DOUBLE) + (mcr_eth / CAST(5800000 AS DOUBLE)) * power(
            (
              CAST(
                running_net_eth + running_net_enzyme + running_net_lido + (
                  CAST(running_net_dai AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) + (
                  CAST(running_net_rpl AS DOUBLE) * CAST(rpl_price_dollar AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) AS DOUBLE
              ) / CAST(mcr_eth AS DOUBLE)
            ),
            4
          ) AS DOUBLE
        )
      END AS nxm_token_price_in_eth,
      CASE
        WHEN mcr_eth = 0 THEN 0
        ELSE CAST(
          CAST(0.01028 AS DOUBLE) + (mcr_eth / CAST(5800000 AS DOUBLE)) * power(
            (
              CAST(
                running_net_eth + running_net_enzyme + running_net_lido + (
                  CAST(running_net_dai AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) + (
                  CAST(running_net_rpl AS DOUBLE) * CAST(rpl_price_dollar AS DOUBLE) / CAST(eth_price_dollar AS DOUBLE)
                ) AS DOUBLE
              ) / CAST(mcr_eth AS DOUBLE)
            ),
            4
          ) AS DOUBLE
        ) * CAST(eth_price_dollar AS DOUBLE)
      END as nxm_token_price_in_dollar
    FROM
      summed
    ORDER BY
      day
  )
SELECT
  *
FROM
  nxm_token_price