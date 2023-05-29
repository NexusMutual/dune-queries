WITH
  eth_daily_transactions AS (
    SELECT DISTINCT
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
      name IN ('Maker: dai', 'Lido: steth')
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
        name IN ('Maker: dai', 'Lido: steth')
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
    SELECT DISTINCT
      date_trunc('day', evt_block_time) AS day,
      AVG(CAST(answer AS double) / 1e18) OVER (
        PARTITION BY
          date_trunc('day', evt_block_time)
      ) AS nxmty_price
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
  ethereum_price_ma7 AS (
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
      ) AS moving_average_dai
    from
      dai_day_prices
    ORDER BY
      day DESC
  ),
  price_ma AS (
    select
      ethereum_price_ma7.day,
      eth_price_dollar,
      dai_price_dollar,
      ethereum_price_ma7.moving_average_eth,
      dai_price_ma7.moving_average_dai
    from
      ethereum_price_ma7
      INNER JOIN dai_price_ma7 ON ethereum_price_ma7.day = dai_price_ma7.day
  ),
  MCR_event AS (
    select
      date_trunc('day', evt_block_time) AS day,
      RANK() OVER (
        PARTITION BY
          date_trunc('day', evt_block_time)
        ORDER BY
          evt_block_time DESC
      ) AS day_rank,
      mcrEtherx100 * 1E-18 AS mcr_eth
    from
      nexusmutual_ethereum.MCR_evt_MCREvent
  ),
  MCR_updated AS (
    select
      date_trunc('day', evt_block_time) AS day,
      RANK() OVER (
        PARTITION BY
          date_trunc('day', evt_block_time)
        ORDER BY
          evt_block_time DESC
      ) AS day_rank,
      mcr * 1E-18 AS mcr_eth
    from
      nexusmutual_ethereum.MCR_evt_MCRUpdated
  ),
  MCR_updated_event AS (
    select
      day,
      mcr_eth
    from
      MCR_event
    WHERE
      day_rank = 1
    UNION
    select
      day,
      mcr_eth
    from
      MCR_updated
    WHERE
      day_rank = 1
  ),
  all_running_totals AS (
    select DISTINCT
      price_ma.day AS day,
      eth_price_dollar,
      dai_price_dollar,
      moving_average_eth,
      moving_average_dai,
      eth_ingress,
      eth_egress,
      net_eth AS running_net_eth,
      net_enzyme,
      nxmty_price,
      dai_net_total,
      lido_ingress,
      mcr_eth
    from
      price_ma
      LEFT JOIN nxmty ON price_ma.day = nxmty.day
      LEFT JOIN dai_transactions ON price_ma.day = dai_transactions.day
      LEFT JOIN steth ON price_ma.day = steth.day
      LEFT JOIN eth_daily_transactions ON price_ma.day = eth_daily_transactions.day
      LEFT JOIN MCR_updated_event ON price_ma.day = MCR_updated_event.day
  ),
  min_count_lagged AS (
    select
      day,
      eth_price_dollar,
      dai_price_dollar,
      moving_average_eth,
      moving_average_dai,
      running_net_eth,
      dai_net_total,
      net_enzyme,
      nxmty_price,
      lido_ingress,
      mcr_eth,
      COUNT(running_net_eth) OVER (
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
      moving_average_eth,
      moving_average_dai,
      COALESCE(
        FIRST_VALUE(running_net_eth) OVER (
          PARTITION BY
            running_net_eth_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_eth,
      COALESCE(
        FIRST_VALUE(dai_net_total) OVER (
          PARTITION BY
            dai_net_total_min_count
          ORDER BY
            day
        ),
        0
      ) AS running_net_dai,
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
      moving_average_eth,
      moving_average_dai,
      SUM(running_net_eth) OVER (
        ORDER BY
          day
      ) AS running_net_eth,
      SUM(running_net_dai) OVER (
        ORDER BY
          day
      ) AS running_net_dai,
      SUM(running_net_enzyme * nxmty_price) OVER (
        ORDER BY
          day
      ) AS running_net_enzyme,
      running_net_lido,
      mcr_eth
    FROM
      lagged
  ),
  nxm_token_price AS (
    SELECT
      day,
      eth_price_dollar,
      dai_price_dollar,
      moving_average_eth,
      moving_average_dai,
      mcr_eth,
      running_net_eth,
      running_net_enzyme,
      running_net_lido,
      running_net_dai / moving_average_eth AS running_net_dai,
      (
        running_net_eth + running_net_enzyme + running_net_lido + (running_net_dai / moving_average_eth)
      ) AS total,
      0.01028 + (mcr_eth / 5800000) * power(
        (
          (
            running_net_eth + running_net_enzyme + running_net_lido + (running_net_dai / moving_average_eth)
          ) / mcr_eth
        ),
        4
      ) AS nxm_token_price_in_eth,
      0.01028 + (mcr_eth / 5800000) * power(
        (
          (
            running_net_eth + running_net_enzyme + running_net_lido + (running_net_dai / moving_average_eth)
          ) / mcr_eth
        ),
        4
      ) * moving_average_eth AS nxm_token_price_in_dollar
    FROM
      summed
    ORDER BY
      day
  ),
  premiums AS (
    SELECT
      DATE_TRUNC('day', CAST(t.call_block_time AS TIMESTAMP)) AS cover_start_time,
      eth_price_dollar,
      dai_price_dollar,
      nxm_token_price_in_eth,
      nxm_token_price_in_dollar,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 0 THEN 'ETH'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 1 THEN 'DAI'
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 255 THEN 'NXM'
        ELSE 'NA'
      END AS premium_asset,
      CASE
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 0 THEN CAST(output_premium AS DOUBLE) * 1E-18 * f.moving_average_eth
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 1 THEN CAST(output_premium AS DOUBLE) * 1E-18 * f.moving_average_dai
        WHEN CAST(JSON_QUERY(params, 'lax $.paymentAsset') AS INT) = 255 THEN CAST(output_premium AS DOUBLE) * 1E-18 * f.nxm_token_price_in_dollar
        ELSE 0
      END AS premium_in_dollar
    FROM
      nexusmutual_ethereum.Cover_call_buyCover AS t
      INNER JOIN nexusmutual_ethereum.StakingProducts_call_getPremium AS u ON t.call_tx_hash = u.call_tx_hash
      INNER JOIN nxm_token_price AS f ON f.day = DATE_TRUNC('day', CAST(t.call_block_time AS TIMESTAMP))
    WHERE
      t.call_success
      AND u.call_success
      AND u.contract_address = 0xcafea573fbd815b5f59e8049e71e554bde3477e4
      AND (
        t.call_trace_address IS NULL
        OR SLICE(
          u.call_trace_address,
          1,
          cardinality(t.call_trace_address)
        ) = t.call_trace_address
      )
    UNION ALL
    SELECT
      DATE_TRUNC('day', evt_block_time) AS cover_start_time,
      eth_price_dollar,
      dai_price_dollar,
      nxm_token_price_in_eth,
      nxm_token_price_in_dollar,
      CASE
        WHEN b.call_success
        OR c.payWithNXM THEN 'NXM'
        WHEN curr = 0x45544800 THEN 'ETH'
        WHEN curr = 0x44414900 THEN 'DAI'
      END AS premium_asset,
      CASE
        WHEN curr = 0x45544800 THEN premium * 1E-18 * eth_price_dollar
        WHEN curr = 0x44414900 THEN premium * 1E-18 * dai_price_dollar
      END AS premium_in_dollar
    FROM
      nexusmutual_ethereum.QuotationData_evt_CoverDetailsEvent AS a
      LEFT JOIN nexusmutual_ethereum.Quotation_call_makeCoverUsingNXMTokens AS b ON a.evt_tx_hash = b.call_tx_hash
      LEFT JOIN nexusmutual_ethereum.Quotation_call_buyCoverWithMetadata AS c ON a.evt_tx_hash = c.call_tx_hash
      INNER JOIN nxm_token_price AS f ON f.day = DATE_TRUNC('day', a.evt_block_time)
    WHERE
      (
        b.call_success
        AND b.call_tx_hash IS NOT NULL
      )
      OR b.call_tx_hash IS NULL
  ),
  premiums_coverted AS (
    SELECT
      year(cover_start_time) AS year,
      quarter(cover_start_time) AS quarter,
      eth_price_dollar,
      dai_price_dollar,
      nxm_token_price_in_eth,
      nxm_token_price_in_dollar,
      premium_in_dollar,
      premium_in_dollar / eth_price_dollar AS premium_in_eth,
      CASE
        WHEN premium_asset = 'DAI' THEN premium_in_dollar
        ELSE 0
      END AS dai_premium_dollar,
      CASE
        WHEN premium_asset = 'DAI' THEN premium_in_dollar / eth_price_dollar
        ELSE 0
      END AS dai_premium_in_eth,
      CASE
        WHEN premium_asset = 'ETH' THEN premium_in_dollar
        ELSE 0
      END AS eth_premium_dollar,
      CASE
        WHEN premium_asset = 'ETH' THEN premium_in_dollar / eth_price_dollar
        ELSE 0
      END AS eth_premium_in_eth,
      CASE
        WHEN premium_asset = 'NXM' THEN premium_in_dollar
        ELSE 0
      END AS nxm_premium_dollar,
      CASE
        WHEN premium_asset = 'NXM' THEN premium_in_dollar / eth_price_dollar
        ELSE 0
      END AS nxm_premium_in_eth
    FROM
      premiums AS t
  )
SELECT DISTINCT
  year,
  quarter,
  concat('Q', CAST(quarter AS VARCHAR)) AS quarter_label,
  CASE
    WHEN '{{display_currency}}' = 'ETH' THEN SUM(premium_in_eth) OVER (
      PARTITION by
        year,
        quarter
    )
    WHEN '{{display_currency}}' = 'USD' THEN SUM(premium_in_dollar) OVER (
      PARTITION by
        year,
        quarter
    )
  END AS quarterly_premium,
  CASE
    WHEN '{{display_currency}}' = 'ETH' THEN SUM(premium_in_eth) OVER (
      PARTITION by
        year
    )
    WHEN '{{display_currency}}' = 'USD' THEN SUM(premium_in_dollar) OVER (
      PARTITION by
        year
    )
  END AS annual_premium,
  CASE
    WHEN '{{display_currency}}' = 'ETH' THEN SUM(premium_in_eth) OVER ()
    WHEN '{{display_currency}}' = 'USD' THEN SUM(premium_in_dollar) OVER ()
  END AS total_premium,
  CASE
    WHEN '{{display_currency}}' = 'ETH' THEN SUM(eth_premium_in_eth) OVER ()
    WHEN '{{display_currency}}' = 'USD' THEN SUM(eth_premium_dollar) OVER ()
  END AS eth_premium,
  CASE
    WHEN '{{display_currency}}' = 'ETH' THEN SUM(dai_premium_in_eth) OVER ()
    WHEN '{{display_currency}}' = 'USD' THEN SUM(dai_premium_dollar) OVER ()
  END AS dai_premium,
  CASE
    WHEN '{{display_currency}}' = 'ETH' THEN SUM(nxm_premium_in_eth) OVER ()
    WHEN '{{display_currency}}' = 'USD' THEN SUM(nxm_premium_dollar) OVER ()
  END AS nxm_premium
FROM
  premiums_coverted
ORDER BY
  year,
  quarter ASC